#include <iostream>
#include <cstdio>
#include <fstream>
#include <cassert>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <stack>
#include <filesystem>

#include "utils.h"

using PrintCellFunc = void(*)(uint16_t);
struct BTreePage;
struct DB;
struct Payload;

enum class BTreePageType : uint8_t {
    InteriorIndexBTreePage = 0x02,
    InteriorTableBTreePage = 0x05,
    LeafIndexBTreePage = 0x0a,
    LeafTableBTreePage = 0x0d,
    Invalid = 0x00
};

enum class ReturnCodes {
    CellFound,
    CellNotFound,
    CellInserted,
    NotEnoughSpaceToInsert,
    RowidAlreadyInDatabase,
    BadSearch,
    NotImplemented,
    EverythingWrong
};

enum class SchemaTypeColumn {
    Table,
    Index,
    View,
    Trigger
};

static std::map<std::string, SchemaTypeColumn>SCHEMA_TYPES{
    {"table", SchemaTypeColumn::Table},
    {"view", SchemaTypeColumn::View},
    {"index", SchemaTypeColumn::Index},
    {"trigger", SchemaTypeColumn::Trigger}
};

enum class ColumnAffinity {
    TEXT,
    INTEGER,
    NUMERIC,
    REAL,
    BLOB
};

static std::map<std::string, ColumnAffinity>AFFINITY{
    {"TEXT", ColumnAffinity::TEXT},
    {"INTEGER", ColumnAffinity::INTEGER},
    {"NUMERIC", ColumnAffinity::NUMERIC},
    {"BLOB", ColumnAffinity::BLOB},
    {"REAL", ColumnAffinity::REAL}
};

enum class ColumnType {
    NULL_,
    INT8,
    BIG_ENDIAN_INT16,
    BIG_ENDIAN_INT24,
    BIG_ENDIAN_INT32,
    BIG_ENDIAN_INT48,
    BIG_ENDIAN_INT64,
    BIG_ENDIAN_IEEE_754_2008_FLOAT64,
    ZERO,
    ONE,
    RESERVED,
    BLOB,
    STRING
};

struct BTreePage {
    struct Header {
        BTreePageType page_type;
        uint16_t first_free_block;
        uint16_t num_of_cells;
        uint16_t start_of_cell_content_area;
        uint8_t num_of_fragmented_free_bytes_in_cell_content;
        uint32_t right_most_pointer;
    };

    Header header;
    bool is_first_page = false;
    DB* db;
    uint8_t* bytes = nullptr;

    BTreePage(DB* db);
    BTreePage(DB* db, uint32_t pg_n);
    BTreePage(DB* db, BTreePageType page_type);
    ~BTreePage();
    void recreate(uint32_t pg_n);
    void recreate(BTreePageType page_type);

    uint16_t get_cell_content_offset(uint16_t idx) {
        uint16_t cell_content_offset;
        read_big_endian16(&cell_content_offset, bytes + get_header_size() + 2 * idx);
        return cell_content_offset;
    }
    uint8_t write_cell_content_offset(uint16_t idx, uint16_t cell_content_offset) {
        if (idx >= header.num_of_cells) {
            std::cout << "out of range\n";
        }
        write_big_endian16(cell_content_offset, bytes + get_header_size() + 2 * idx);
        return 2;
    }

    BTreePageType get_page_type(uint8_t page_type);
    uint16_t compute_directly_stored_payload_size(uint64_t P);
    uint16_t compute_free_space();
    uint16_t compute_cell_size(uint64_t id, uint64_t P = 0);
    uint64_t get_cell_rowid(uint16_t offset);
    uint64_t get_cell_payload_size(uint16_t offset);
    uint32_t get_cell_left_child_pointer(uint16_t offset);
    uint32_t get_cell_first_overflow_page(uint16_t offset);
    uint8_t get_header_size();
    uint32_t get_right_most_pointer();
    uint16_t lower_bound(uint64_t id);
    bool compare_rowid(uint16_t idx, uint64_t id);
    uint16_t min_payload();
    uint16_t max_payload();
    uint16_t get_split_index(uint16_t idx, uint16_t* sums, uint16_t* cell_sizes, uint16_t* cell_content_offsets);
    void read_cell(uint16_t offset, Payload* p);

    ReturnCodes insert_leaf_cell(uint64_t id, uint16_t cell_offsets_idx, Payload* payload);
    ReturnCodes insert_interior_cell(uint64_t id, uint16_t cell_offsets_idx, uint32_t left_child_pointer);
    void shift_cell_offsets_array(uint16_t idx);
    void write_num_of_cells() { write_big_endian16(header.num_of_cells, bytes + 1 + 2); } //uint16_t check; read_big_endian16(&check, bytes + 1 + 2); std::cout << "num_of_cells: " << check; }
    void write_start_of_cell_content_area() { write_big_endian16(header.start_of_cell_content_area, bytes + 1 + 2 + 2); } // uint16_t check; read_big_endian16(&check, bytes + 1 + 2 + 2); std::cout << "start_of_cell_content_area: " << check; }//
    void write_header();

    void info();
    void print_header();
    void print_cell_offsets_array(PrintCellFunc func);
    void print_cell_offsets_array();
    void print_leaf_cell_rowid(uint16_t offset);
    void print_type();
    void print();
    void print_cell(uint16_t offset);
};

struct Payload {
    uint64_t P;
    uint8_t* bytes;
    uint64_t rowid;

    Payload(std::string& map);
    Payload(uint64_t P);
    Payload(): P(0), bytes(nullptr), rowid(0) { }
    ~Payload();
    void recreate(uint64_t P, uint64_t rowid);
    uint64_t get_bytes_in_header(std::string& map);
    uint64_t get_payload_size(std::string& map);
    uint64_t get_column_content_size(uint64_t serial_type);
    ColumnType get_column_type(uint64_t serial_type);
    std::string get_text_column(uint16_t column_idx);
    int64_t get_integer_column(uint16_t column_idx);
    double get_real_column(uint16_t column_idx);
    void info();
    void print();
    uint64_t print_serial_type_description(uint64_t serial_type);
};

struct DB {
    struct Header {
        uint8_t header_string[16];
        uint16_t page_size;
        uint8_t file_format_write_version;
        uint8_t file_format_read_version;
        uint8_t unused_reserved_space;
        uint8_t max_embedded_payload_fraction;
        uint8_t min_embedded_payload_fraction;
        uint8_t leaf_payload_fraction;
        uint32_t file_change_counter;
        uint32_t database_size_in_pages;
        uint32_t first_freelist_trunk_page;
        uint32_t total_freelist_pages;
        uint32_t schema_cookie;
        uint32_t schema_format_number;
        uint32_t default_page_cache_size;
        uint32_t largest_root_b_tree_page;
        uint32_t database_text_encoding;
        uint32_t user_version;
        uint32_t incremental_vacuum_mode;
        uint32_t application_id;
        uint8_t reserved_expansion[20];
        uint32_t version_valid_for_number;
        uint32_t sqlite_version_number;
    };

    struct TableSchema {
        uint32_t root_pg_n;
        std::map<std::string, uint16_t> columns;
        std::vector<ColumnAffinity> columns_affinity;
    };

    std::fstream file;
    Header header;
    std::map<std::string, TableSchema> tables;

    DB(std::string& fn);

    bool check_inheader_dbsize();
    bool check_split_is_enough(uint16_t split_idx, uint16_t num_of_cells, uint16_t* sums);
    uint32_t compute_database_size_in_pages();
    uint16_t get_page_size();
    uint16_t get_U();
    uint32_t get_root_page_number(std::string& table_name);
    void parse_schema();
    void parse_create_table_sql(const std::string& sql);
    void parse_select_sql(const std::string& sql);
    void parse_insert_sql(const std::string& sql);
    ReturnCodes find(uint32_t root_pg_n, uint64_t id, Payload* p);

    void write(uint32_t pg_n, uint8_t* bytes);
    ReturnCodes insert(uint32_t root_pg_n, uint64_t id, Payload* payload);

    void read_header();
    void print_schema_format_description(int format);
    void print_encoding(int num);
    void print_header();
    void print_tree(uint32_t root_pg_n);
};

struct Parser {
    Lexer& lex;
    DB* db;
    Payload* p;
    std::string& table_name;

    bool match(Tag expected);
    void error(const std::string& message);
    bool parse_where(Payload* p);
    bool parse_values(Payload* p);
    bool parse_or();
    bool parse_and();
    bool parse_comparison();
    void restart(size_t i = 0);

    Parser(Lexer& lex, DB* db, std::string& table_name);
};

// enum class Tag {
//     INTEGER_LITERAL,
//     REAL_LITERAL,
//     STRING_LITERAL,
//     TYPE_TEXT,
//     TYPE_INTEGER,
//     TYPE_NUMERIC,
//     TYPE_BLOB,
//     TYPE_REAL,
//     CREATE,
//     TABLE,
//     INSERT,
//     INTO,
//     VALUES,
//     SELECT,
//     FROM,
//     WHERE,
//     AND,
//     OR,
//     LESS,
//     LESS_OR_EQUAL,
//     GREATER,
//     GREATER_OR_EQUAL,
//     NOT_EQUAL,
//     EQUAL,
//     UNARY_MINUS,
//     LEFT_BRACKET,
//     RIGHT_BRACKET,
//     EOF_TOKEN,
//     ERROR,
//     COMMA,
//     ALL
// };

// class Token {
// public:
//     Tag tag;
//     Token(): tag(Tag::EOF_TOKEN) { }
//     Token(Tag t): tag(t) { }
//     Token(const Token& other): tag(other.tag) { }
//     virtual ~Token() { }
//     Token& operator=(const Token& other) {
//         tag = other.tag;
//         return *this;
//     }
// };

// struct IntegerLiteral: Token {
//     int64_t value;
//     IntegerLiteral(int64_t v): Token(Tag::INTEGER_LITERAL), value(v) {}
// };

// struct RealLiteral: Token {
//     double value;
//     RealLiteral(double v): Token(Tag::REAL_LITERAL), value(v) {}
// };

// struct StringLiteral: Token {
//     std::string value;
//     StringLiteral(const std::string& s): Token(Tag::STRING_LITERAL), value(s) {}
// };

// struct Lexer {
//     int line = 1;
//     size_t i = 0;
//     char peek = ' ';
//     const std::string& s;
//     Token* cur = nullptr;

//     void next_char();
//     bool next_char_and_compare(char c);
//     Lexer(const std::string& s): s(s) { }
//     ~Lexer();
//     void restart(size_t i = 0);
//     Token* scan();
// };



DB::DB(std::string& fn) {

    if (!std::filesystem::exists(fn)) {
        std::cerr << "you would die\n";
        file.open(fn, std::ios::binary | std::ios::in | std::ios::out);
        return;
    }

    file.open(fn, std::ios::binary | std::ios::in | std::ios::out);

    if (!file.is_open()) {
        std::cerr << "everything is completely wrong\n";
        return;
    }

    uint8_t* bytes = new uint8_t[100];
    file.read(reinterpret_cast<char*>(bytes), 100);

    uint16_t offset = 16;
    offset += read_big_endian16(&header.page_size, bytes + offset);
    offset += read_big_endian8(&header.file_format_write_version, bytes + offset);
    offset += read_big_endian8(&header.file_format_read_version, bytes + offset);
    offset += read_big_endian8(&header.unused_reserved_space, bytes + offset);
    offset += read_big_endian8(&header.max_embedded_payload_fraction, bytes + offset);
    offset += read_big_endian8(&header.min_embedded_payload_fraction, bytes + offset);
    offset += read_big_endian8(&header.leaf_payload_fraction, bytes + offset);
    offset += read_big_endian32(&header.file_change_counter, bytes + offset);
    offset += read_big_endian32(&header.database_size_in_pages, bytes + offset);
    offset += read_big_endian32(&header.first_freelist_trunk_page, bytes + offset);
    offset += read_big_endian32(&header.total_freelist_pages, bytes + offset);
    offset += read_big_endian32(&header.schema_cookie, bytes + offset);
    offset += read_big_endian32(&header.schema_format_number, bytes + offset);
    offset += read_big_endian32(&header.default_page_cache_size, bytes + offset);
    offset += read_big_endian32(&header.largest_root_b_tree_page, bytes + offset);
    offset += read_big_endian32(&header.database_text_encoding, bytes + offset);
    offset += read_big_endian32(&header.user_version, bytes + offset);
    offset += read_big_endian32(&header.incremental_vacuum_mode, bytes + offset);
    offset += read_big_endian32(&header.application_id, bytes + offset);
    offset += 20;
    offset += read_big_endian32(&header.version_valid_for_number, bytes + offset);
    offset += read_big_endian32(&header.sqlite_version_number, bytes + offset);
    delete[] bytes;

    parse_schema();
}

void DB::write(uint32_t pg_n, uint8_t* bytes) {
    file.seekp((pg_n - 1) * get_page_size(), std::ios::beg);
    file.write(reinterpret_cast<char*>(bytes), get_page_size());
    
    file.seekp(28, std::ios::beg);
    uint8_t buffer[4];
    write_big_endian32(header.database_size_in_pages, buffer);
    file.write(reinterpret_cast<char*>(buffer), 4);
}

uint32_t DB::compute_database_size_in_pages() {
    file.seekg(0, std::ios::end);
    std::streampos file_size = file.tellg();

    if (file_size == -1) {
        std::cerr << "error determining file size.\n";
        return 0;
    }
    return static_cast<uint32_t>(file_size) / get_page_size();
}

void DB::parse_create_table_sql(const std::string& sql) {
    Lexer lexer(sql);
    Token* token = lexer.scan();
    std::string table_name;
    uint16_t idx = 0;

    while (token->tag != Tag::EOF_TOKEN && token->tag != Tag::ERROR) {
        if (token->tag == Tag::CREATE) {
            token = lexer.scan();
            if (token->tag != Tag::TABLE) {
                return;
            }
            token = lexer.scan();
            if (token->tag != Tag::STRING_LITERAL) {
                return;
            }
            StringLiteral* s = static_cast<StringLiteral*>(token);
            table_name = s->value;
        } else if (token->tag == Tag::STRING_LITERAL) {
            StringLiteral* s = static_cast<StringLiteral*>(token);
            std::string column_name = s->value;
            token = lexer.scan();
            if (token->tag == Tag::TYPE_TEXT) {
                tables[table_name].columns[column_name] = idx;
                tables[table_name].columns_affinity.push_back(ColumnAffinity::TEXT);
            } else if (token->tag == Tag::TYPE_INTEGER) {
                tables[table_name].columns[column_name] = idx;
                tables[table_name].columns_affinity.push_back(ColumnAffinity::INTEGER);
            }
        }
        if (token->tag == Tag::COMMA) {
            ++idx;
        }
        token = lexer.scan();
    }
}

void DB::parse_select_sql(const std::string& sql) {
    Lexer lexer(sql);
    Token* token = lexer.scan();
    std::vector<std::string> columns;
    std::string table_name, column_name;
    bool select_all = false;
    bool condition = false;
    uint16_t idx = 0;

    if (token->tag != Tag::SELECT) {
        std::cout << "need SELECT\n";
        return;
    }

    while (token->tag != Tag::FROM) {
        token = lexer.scan();
        if (token->tag == Tag::STRING_LITERAL) {
            column_name = static_cast<StringLiteral*>(token)->value;
            columns.push_back(column_name);
        } else if (token->tag == Tag::ALL) {
            select_all = true;
        } else if (token->tag == Tag::EOF_TOKEN || token->tag == Tag::ERROR) {
            return;
        } else if (token->tag == Tag::COMMA) {
            ++idx;
        }
    }

    token = lexer.scan();

    if (token->tag != Tag::STRING_LITERAL) {
        std::cout << "need table name\n";
        return;
    }
    table_name = static_cast<StringLiteral*>(token)->value;

    if (tables.count(table_name) == 0) {
        std::cout << "no table name " << table_name << " in schema\n";
        return;
    }

    token = lexer.scan();

    if (token->tag == Tag::WHERE) {
        condition = true;
    }

    BTreePage root(this);
    Payload p;
    std::stack<uint32_t> stack;
    stack.push(tables[table_name].root_pg_n);
    uint16_t cell_content_offset;

    size_t condition_i = lexer.i;
    Parser parser(lexer, this, table_name);
    
    while (!stack.empty()) {
        root.recreate(stack.top());
        stack.pop();
        
        if (root.header.page_type == BTreePageType::LeafTableBTreePage) {
            for (uint16_t idx = 0; idx < root.header.num_of_cells; ++idx) {
                cell_content_offset = root.get_cell_content_offset(idx);
                root.read_cell(cell_content_offset, &p);

                if (condition) {
                    parser.restart(condition_i);
                    bool res = parser.parse_where(&p);
                    if (!res) {
                        continue;
                    }
                }
                
                if (select_all) {
                    std::cout << "select all\n";
                    p.print();
                } else {
                    for (std::string column : columns) {
                        if (tables[table_name].columns.count(column) == 0) {
                            std::cout << "no column: " << column << "in table " << table_name << "\n";
                        } else {
                            if (tables[table_name].columns_affinity[tables[table_name].columns[column]] == ColumnAffinity::TEXT) {
                                std::string ans = p.get_text_column(tables[table_name].columns[column] + 1);
                                std::cout << "text column: ";
                                std::cout << ans << "\n";
                            } else if (tables[table_name].columns_affinity[tables[table_name].columns[column]] == ColumnAffinity::INTEGER) {
                                int64_t ans = p.get_integer_column(tables[table_name].columns[column] + 1);
                                std::cout << "integer column: ";
                                std::cout << ans << "\n";
                            }
                        }
                    }
                }
            }
        } else if (root.header.page_type == BTreePageType::InteriorTableBTreePage) {
            for (uint16_t idx = 0; idx < root.header.num_of_cells; ++idx) {
                cell_content_offset = root.get_cell_content_offset(idx);
                stack.push(root.get_cell_left_child_pointer(cell_content_offset));
            }
            stack.push(root.get_right_most_pointer());
        }
    }
}

void DB::parse_insert_sql(const std::string& sql) {
    Lexer lexer(sql);
    Token* token = lexer.scan();
    std::string table_name;

    if (token->tag != Tag::INSERT) {
        std::cout << "need INSERT\n";
        return;
    }

    lexer.scan();

    if (token->tag != Tag::INTO) {
        std::cout << "need INTO\n";
        return;
    }

    token = lexer.scan();

    if (token->tag != Tag::STRING_LITERAL) {
        std::cout << "need table name\n";
        return;
    }
    table_name = static_cast<StringLiteral*>(token)->value;

    if (tables.count(table_name) == 0) {
        std::cout << "no table name " << table_name << " in schema\n";
        return;
    }

    token = lexer.scan();

    if (token->tag != Tag::VALUES) {
        std::cout << "need VALUES\n";
    }

    Parser parser(lexer, this, table_name);
    Payload p;

    bool res = parser.parse_values(&p);

    if (!res) {
        std::cout << "bad values\n";
        return;
    }

    if (p.rowid == 0) {
        std::cout << "everything wrong\n";
        return;
    }

    ReturnCodes rc = insert(tables[table_name].root_pg_n, p.rowid, &p);

    if (rc == ReturnCodes::RowidAlreadyInDatabase) {
        std::cout << "cell with id already in database\n";
        return;
    }

    if (rc != ReturnCodes::CellInserted) {
        std::cout << "everything wrong or triple split\n";
        return;
    }
}

void DB::parse_schema() {
    BTreePage schema(this);
    Payload p;
    std::stack<uint32_t> stack;
    stack.push(1);
    uint16_t cell_content_offset;
    
    while (!stack.empty()) {
        schema.recreate(stack.top());
        stack.pop();
        
        if (schema.header.page_type == BTreePageType::LeafTableBTreePage) {
            for (uint16_t idx = 0; idx < schema.header.num_of_cells; ++idx) {
                cell_content_offset = schema.get_cell_content_offset(idx);
                schema.read_cell(cell_content_offset, &p);
                std::string schema_type = p.get_text_column(1);
                SchemaTypeColumn type = SCHEMA_TYPES[schema_type];
                if (type == SchemaTypeColumn::Table) {
                    std::string table_name = p.get_text_column(2);
                    tables[table_name] = TableSchema();
                    uint64_t root_pg_n = p.get_integer_column(4);
                    tables[table_name].root_pg_n = root_pg_n;
                    std::string sql = p.get_text_column(5);
                    parse_create_table_sql(sql);
                }
            }
        } else if (schema.header.page_type == BTreePageType::InteriorTableBTreePage) {
            for (uint16_t idx = 0; idx < schema.header.num_of_cells; ++idx) {
                cell_content_offset = schema.get_cell_content_offset(idx);
                stack.push(schema.get_cell_left_child_pointer(cell_content_offset));
            }
            stack.push(schema.get_right_most_pointer());
        }
    }
}

uint32_t DB::get_root_page_number(std::string& table_name) {
    return tables[table_name].root_pg_n;
}

bool DB::check_inheader_dbsize() {
    return header.database_size_in_pages > 0 && header.file_change_counter == header.version_valid_for_number;
}

uint16_t DB::get_page_size() {
    return header.page_size;
}

uint16_t DB::get_U() {
    return get_page_size() - header.unused_reserved_space;
}

bool DB::check_split_is_enough(uint16_t split_idx, uint16_t num_of_cells, uint16_t* sums) {
    return sums[split_idx] + 8 + 2 * (split_idx + 1) < get_U() && sums[num_of_cells] - sums[split_idx] + 8 + 2 * (num_of_cells - split_idx - 1) < get_U();
}

ReturnCodes DB::insert(uint32_t root_pg_n, uint64_t id, Payload* payload) {
    uint64_t rowid;
    uint32_t left_child_pointer, right_most_pointer;
    uint16_t cell_content_offset, idx;

    uint32_t current_pg_n = root_pg_n;
    BTreePage current_page(this, current_pg_n);

    uint32_t parents[20];
    uint8_t n_parents = 0;

    while (current_page.header.page_type != BTreePageType::LeafTableBTreePage) {
        parents[n_parents] = current_pg_n;
        n_parents++;

        idx = current_page.lower_bound(id);

        if (idx != current_page.header.num_of_cells) {
            cell_content_offset = current_page.get_cell_content_offset(idx);
            left_child_pointer = current_page.get_cell_left_child_pointer(cell_content_offset);
            current_pg_n = left_child_pointer;
        } else {
            current_pg_n = current_page.get_right_most_pointer();
        }
        current_page.recreate(current_pg_n);
    }

    idx = current_page.lower_bound(id);
    if (idx != current_page.header.num_of_cells) {
        cell_content_offset = current_page.get_cell_content_offset(idx);
        rowid = current_page.get_cell_rowid(cell_content_offset);
        if (rowid == id) {
            return ReturnCodes::RowidAlreadyInDatabase;
        }
    }

    ReturnCodes rc = current_page.insert_leaf_cell(id, idx, payload);
    if (rc == ReturnCodes::CellInserted) {
        write(current_pg_n, current_page.bytes);
        return rc;
    }

    uint16_t sums[current_page.header.num_of_cells + 1];
    uint16_t cell_sizes[current_page.header.num_of_cells + 1];
    uint16_t cell_content_offsets[current_page.header.num_of_cells + 1];

    cell_sizes[idx] = current_page.compute_cell_size(id, payload->P);

    uint16_t split_idx = current_page.get_split_index(idx, sums, cell_sizes, cell_content_offsets);
    uint64_t split_rowid = (split_idx == idx) ? id : current_page.get_cell_rowid(cell_content_offsets[split_idx]);

    BTreePage new_page(this, BTreePageType::LeafTableBTreePage);

    if (check_split_is_enough(split_idx, current_page.header.num_of_cells, sums)) {

        for (uint16_t i = split_idx + 1; i < current_page.header.num_of_cells + 1; ++i) {
            if (i == idx) {
                ReturnCodes rc = new_page.insert_leaf_cell(id, new_page.header.num_of_cells, payload);
                if (rc != ReturnCodes::CellInserted) {
                    return ReturnCodes::EverythingWrong;
                }
            } else {
                uint16_t cell_content_offset = new_page.header.start_of_cell_content_area - cell_sizes[i];
                new_page.header.start_of_cell_content_area -= cell_sizes[i];
                new_page.header.num_of_cells += 1;
                new_page.write_cell_content_offset(new_page.header.num_of_cells - 1, cell_content_offset);
                std::memcpy(new_page.bytes + cell_content_offset, current_page.bytes + cell_content_offsets[i], cell_sizes[i]);
            }
        }
        new_page.write_header();

    } else {
        std::cout << "triple split needed\n";
        new_page.write_header();
        return ReturnCodes::NotImplemented;
    }

    if (current_pg_n == root_pg_n) {
        BTreePage leaf(this, BTreePageType::LeafTableBTreePage);
        for (uint16_t i = 0; i <= split_idx; ++i) {
            if (i == idx) {
                ReturnCodes rc = leaf.insert_leaf_cell(id, idx, payload);
                if (rc != ReturnCodes::CellInserted) {
                    return ReturnCodes::EverythingWrong;
                }
            } else {
                uint16_t cell_content_offset = leaf.header.start_of_cell_content_area - cell_sizes[i];
                leaf.header.start_of_cell_content_area -= cell_sizes[i];
                leaf.header.num_of_cells += 1;
                leaf.write_cell_content_offset(leaf.header.num_of_cells - 1, cell_content_offset);
                std::memcpy(leaf.bytes + cell_content_offset, current_page.bytes + cell_content_offsets[i], cell_sizes[i]);
            }
        }
        leaf.write_header();

        left_child_pointer = compute_database_size_in_pages() + 1;
        right_most_pointer = compute_database_size_in_pages() + 2;
        header.database_size_in_pages += 2;
        write(left_child_pointer, leaf.bytes);
        write(right_most_pointer, new_page.bytes);

        current_page.recreate(BTreePageType::InteriorTableBTreePage);
        current_page.header.num_of_cells = 1;
        current_page.header.start_of_cell_content_area -= current_page.compute_cell_size(split_rowid);
        current_page.header.right_most_pointer = right_most_pointer;

        current_page.write_header();

        uint16_t offset = current_page.header.start_of_cell_content_area;
        write_big_endian16(offset, current_page.bytes + current_page.get_header_size());
        offset += write_big_endian32(left_child_pointer, current_page.bytes + offset);
        offset += write_varint(split_rowid, current_page.bytes + offset);
        write(current_pg_n, current_page.bytes);
        return ReturnCodes::CellInserted;
    } else {
        write(current_pg_n, new_page.bytes);

        new_page.recreate(BTreePageType::LeafTableBTreePage);
        for (uint16_t i = 0; i <= split_idx; ++i) {
            if (i == idx) {
                ReturnCodes rc = new_page.insert_leaf_cell(id, idx, payload);
                if (rc != ReturnCodes::CellInserted) {
                    return ReturnCodes::EverythingWrong;
                }
            } else {
                uint16_t cell_content_offset = new_page.header.start_of_cell_content_area - cell_sizes[i];
                new_page.header.start_of_cell_content_area -= cell_sizes[i];
                new_page.header.num_of_cells += 1;
                new_page.write_cell_content_offset(new_page.header.num_of_cells - 1, cell_content_offset);
                std::memcpy(new_page.bytes + cell_content_offset, current_page.bytes + cell_content_offsets[i], cell_sizes[i]);
            }
        }
        new_page.write_header();

        left_child_pointer = compute_database_size_in_pages() + 1;
        header.database_size_in_pages += 1;
        write(left_child_pointer, new_page.bytes);
    }

    while (current_pg_n != root_pg_n) {

        id = split_rowid;
        n_parents--;
        current_pg_n = parents[n_parents];
        current_page.recreate(current_pg_n);

        idx = current_page.lower_bound(id);
        ReturnCodes rc = current_page.insert_interior_cell(id, idx, left_child_pointer);
        
        if (rc == ReturnCodes::CellInserted) {
            write(current_pg_n, current_page.bytes);
            return ReturnCodes::CellInserted;
        }

        split_idx = current_page.header.num_of_cells / 2;
        if (split_idx < idx) {
            split_rowid = current_page.get_cell_rowid(current_page.get_cell_content_offset(split_idx));
            right_most_pointer = current_page.get_cell_left_child_pointer(current_page.get_cell_content_offset(split_idx));
        } else if (split_idx == idx) {
            split_rowid = id;
            right_most_pointer = left_child_pointer;
        } else {
            split_rowid = current_page.get_cell_rowid(current_page.get_cell_content_offset(split_idx - 1));
            right_most_pointer = current_page.get_cell_left_child_pointer(current_page.get_cell_content_offset(split_idx - 1));
        }

        new_page.recreate(BTreePageType::InteriorTableBTreePage);
        for (uint16_t i = split_idx + 1; i < current_page.header.num_of_cells + 1; ++i) {
            if (i < idx) {
                cell_content_offset = current_page.get_cell_content_offset(i);
                new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
            } else if (i == idx) {
                new_page.insert_interior_cell(i, id, left_child_pointer);
            } else {
                cell_content_offset = current_page.get_cell_content_offset(i - 1);
                new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
            }
        }
        new_page.write_header();


        if (current_pg_n == root_pg_n) {
            break;
        }

        write(current_pg_n, new_page.bytes);

        new_page.recreate(BTreePageType::InteriorTableBTreePage);
        new_page.header.right_most_pointer = right_most_pointer;
        for (uint16_t i = 0; i < split_idx; ++i) {
            if (i < idx) {
                cell_content_offset = current_page.get_cell_content_offset(i);
                new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
            } else if (i == idx) {
                new_page.insert_interior_cell(i, id, left_child_pointer);
            } else {
                cell_content_offset = current_page.get_cell_content_offset(i - 1);
                new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
            }
        }
        new_page.write_header();

        left_child_pointer = compute_database_size_in_pages() + 1;
        header.database_size_in_pages += 1;
        write(left_child_pointer, new_page.bytes);
    }

    // need new root
    uint32_t left_pointer = compute_database_size_in_pages() + 1;
    uint32_t right_pointer = compute_database_size_in_pages() + 2;
    header.database_size_in_pages += 2;
    write(right_pointer, new_page.bytes);

    new_page.recreate(BTreePageType::InteriorTableBTreePage);
    new_page.header.right_most_pointer = right_most_pointer;
    for (uint16_t i = 0; i < split_idx; ++i) {
        if (i < idx) {
            cell_content_offset = current_page.get_cell_content_offset(i);
            new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
        } else if (i == idx) {
            new_page.insert_interior_cell(i, id, left_child_pointer);
        } else {
            cell_content_offset = current_page.get_cell_content_offset(i - 1);
            new_page.insert_interior_cell(i, current_page.get_cell_rowid(cell_content_offset), current_page.get_cell_left_child_pointer(cell_content_offset));
        }
    }
    new_page.write_header();

    write(left_pointer, new_page.bytes);

    current_page.recreate(BTreePageType::InteriorTableBTreePage);
    current_page.header.num_of_cells = 1;
    current_page.header.start_of_cell_content_area -= current_page.compute_cell_size(split_rowid);
    current_page.header.right_most_pointer = right_pointer;
    current_page.write_header();
    uint16_t offset = current_page.header.start_of_cell_content_area;
    write_big_endian16(offset, current_page.bytes + current_page.get_header_size());
    offset += write_big_endian32(left_pointer, current_page.bytes + offset);
    offset += write_varint(split_rowid, current_page.bytes + offset);
    write(current_pg_n, current_page.bytes);

    return ReturnCodes::CellInserted;
}

ReturnCodes DB::find(uint32_t root_pg_n, uint64_t id, Payload* p) {
    uint32_t current_pg_n = root_pg_n;
    BTreePage current_page(this, current_pg_n);

    while (current_page.header.page_type != BTreePageType::LeafTableBTreePage) {
        uint16_t idx = current_page.lower_bound(id);

        if (idx != current_page.header.num_of_cells) {
            uint16_t cell_content_offset = current_page.get_cell_content_offset(idx);
            uint32_t left_child_pointer = current_page.get_cell_left_child_pointer(cell_content_offset);
            current_pg_n = left_child_pointer;
        } else {
            current_pg_n = current_page.get_right_most_pointer();
        }

        current_page.recreate(current_pg_n);
    }

    uint16_t idx = current_page.lower_bound(id);

    if (idx == current_page.header.num_of_cells) {
        return ReturnCodes::CellNotFound;
    }

    uint16_t cell_content_offset = current_page.get_cell_content_offset(idx);
    uint64_t rowid = current_page.get_cell_rowid(cell_content_offset);

    if (rowid != id) {
        return ReturnCodes::CellNotFound;
    }
    
    current_page.read_cell(cell_content_offset, p);
    return ReturnCodes::CellFound;
}

void DB::print_tree(uint32_t root_pg_n) {
    if (header.database_size_in_pages <= 1) {
        return;
    }

    BTreePage root(this, root_pg_n);
    if (root.header.page_type != BTreePageType::InteriorTableBTreePage) {
        root.print();
        return;
    }

    uint16_t offset = root.get_header_size();
    root.print_header();
    uint64_t rowid;
    uint32_t left_child_pointer;
    uint16_t cell_content_offset;
    for (int i = 0; i < root.header.num_of_cells; ++i) {
        read_big_endian16(&cell_content_offset, root.bytes + offset + 2 * i);
        read_big_endian32(&left_child_pointer, root.bytes + cell_content_offset);
        std::cout << "\t\t/ [" << left_child_pointer << "]\t";

        BTreePage leaf(this, left_child_pointer);
        leaf.print_cell_offsets_array();

        std::cout << "\n";

        read_varint(&rowid, root.bytes + cell_content_offset + 4);
        std::cout << "[" << rowid << "]\n";
    }

    std::cout << "\t\t\\ [" << root.get_right_most_pointer() << "]\t";
    BTreePage leaf(this, root.get_right_most_pointer());
    leaf.print_cell_offsets_array();
    std::cout << "\n";
}

void DB::print_schema_format_description(int format) {
    switch (format) {
        case 1:
            std::cout << "format 1 is understood by all versions of SQLite back to version 3.0.0 (2004-06-18).\n";
            break;
        case 2:
            std::cout << "format 2 adds the ability of rows within the same table to have a varying number of columns, \n"
                         "\tin order to support the ALTER TABLE ... ADD COLUMN functionality. \n"
                         "\tsupport for reading and writing format 2 was added in SQLite version 3.1.3 on 2005-02-20.\n";
            break;
        case 3:
            std::cout << "format 3 adds the ability of extra columns added by ALTER TABLE ... ADD COLUMN \n"
                         "\tto have non-NULL default values. \n"
                         "\tthis capability was added in SQLite version 3.1.4 on 2005-03-11.\n";
            break;
        case 4:
            std::cout << "format 4 causes SQLite to respect the DESC keyword on index declarations. \n"
                         "\t(The DESC keyword is ignored in indexes for formats 1, 2, and 3.) \n"
                         "\tformat 4 also adds two new boolean record type values (serial types 8 and 9). \n"
                         "\tsupport for format 4 was added in SQLite 3.3.0 on 2006-01-10.\n";
            break;
        default:
            std::cout << "unknown format.\n";
    }
}

void DB::print_encoding(int num) {
    switch (num) {
        case 1:
            std::cout << "UTF-8\n";
            break;
        case 2:
            std::cout << "UTF-16le\n";
            break;
        case 3:
            std::cout << "UTF-16be\n";
            break;
        default:
            std::cerr << "invalid encoding value.\n";
    }
}

void DB::print_header() {
    std::cout << "\n--- SQLite header ---\n\n";
    std::cout << "header string: " << header.header_string << "\n";
    std::cout << "page size: " << header.page_size << " bytes [info: 1 means 65536]\n";
    std::cout << "file format write version: " << header.file_format_write_version << " [info: 1 means rollback journaling, 2 WAL]\n";
    std::cout << "file format read version: " << header.file_format_read_version << " [info: if read version is 1 or 2 and write is greater when database is readonly, if read is > 2 when database cannot be read or write]\n";
    std::cout << "unused reserved space: " << header.unused_reserved_space << " [info: usually 0]\n";
    std::cout << "max embedded payload fraction: " << header.max_embedded_payload_fraction << " [info: must be 64]\n";
    std::cout << "min embedded payload fraction: " << header.min_embedded_payload_fraction << " [info: must be 32]\n";
    std::cout << "leaf payload fraction: " << header.leaf_payload_fraction << " [info: must be 32]\n";
    std::cout << "file change counter: " << header.file_change_counter << "\n";
    std::cout << "database size in pages: " << header.database_size_in_pages << " [info: need check may be invalid]\n";
    std::cout << (check_inheader_dbsize() ? "\tvalid inheader size\n" : "\tinvalid inheader size\n");
    std::cout << "first freelist trunk page: " << header.first_freelist_trunk_page << " [info: 0 if no free lists]\n";
    std::cout << "total freelist pages: " << header.total_freelist_pages << "\n";
    std::cout << "schema cookie: " << header.schema_cookie << " [info: increments then SCHEMA changes]\n";
    std::cout << "schema format number: " << header.schema_format_number << "\n\t";
    print_schema_format_description(header.schema_format_number);
    std::cout << "default page cache size: " << header.default_page_cache_size << "\n";
    std::cout << "largest root b-tree page: " << header.largest_root_b_tree_page << "\n";
    std::cout << "database text encoding: " << header.database_text_encoding << "\n\t";
    print_encoding(header.database_text_encoding);
    std::cout << "user version: " << header.user_version << " [info: not used by sqlite]\n";
    std::cout << "incremental vacuum mode: " << header.incremental_vacuum_mode << "\n";
    std::cout << "application id: " << header.application_id << "\n";
    std::cout << "reserved expansion: " << header.reserved_expansion << "\n";
    std::cout << "version valid for number: " << header.version_valid_for_number << "\n";
    std::cout << "sqlite version number: " << header.sqlite_version_number << "\n";
    std::cout << "\n--- end SQLite header ---\n\n";
}


uint16_t BTreePage::min_payload() {
    uint16_t U = db->get_U();
    uint16_t M = ((U - 12) * 32 / 255) - 23;
    return M;
}

uint16_t BTreePage::max_payload() {
    uint16_t U = db->get_U();
    uint16_t X;

    if (header.page_type == BTreePageType::LeafTableBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) {
        X = U - 35;
    }
    else if (header.page_type == BTreePageType::InteriorIndexBTreePage || header.page_type == BTreePageType::LeafIndexBTreePage) {
        X = ((U - 12) * 64 / 255) - 23;
    }

    return X;
}

uint16_t BTreePage::compute_free_space() {
    return header.start_of_cell_content_area - (2 * header.num_of_cells + get_header_size());
}

uint16_t BTreePage::compute_directly_stored_payload_size(uint64_t P) { // P: payload size in bytes
    uint16_t U = db->get_U();
    uint16_t M = min_payload();
    uint16_t X = max_payload();
    uint16_t K = M + ((P - M) % (U - 4));

    if (P <= X) {
        return P;
    }
    // (P > X)
    if (K <= X) {
        return K;
    } // K > X
    return M;
}

BTreePage::BTreePage(DB* db, uint32_t pg_n): db(db), bytes(new uint8_t[db->get_page_size()]) {
    db->file.seekg((pg_n - 1) * db->get_page_size(), std::ios::beg);
    db->file.read(reinterpret_cast<char*>(bytes), db->get_page_size());

    uint16_t offset = 0;
    if (pg_n == 1) {
        is_first_page = true;
        offset = 100;
    }
    uint8_t page_type;
    offset += read_big_endian8(&page_type, bytes + offset);
    header.page_type = get_page_type(page_type);
    offset += read_big_endian16(&header.first_free_block, bytes + offset);
    offset += read_big_endian16(&header.num_of_cells, bytes + offset);
    offset += read_big_endian16(&header.start_of_cell_content_area, bytes + offset);
    offset += read_big_endian8(&header.num_of_fragmented_free_bytes_in_cell_content, bytes + offset);
    if (header.page_type == BTreePageType::InteriorIndexBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) {
        read_big_endian32(&header.right_most_pointer, bytes + offset);
    } else {
        header.right_most_pointer = 0;
    }
}

BTreePage::BTreePage(DB* db, BTreePageType page_type): db(db), bytes(new uint8_t[db->get_page_size()]) {
    header.page_type = page_type;
    header.first_free_block = 0;
    header.num_of_cells = 0;
    header.start_of_cell_content_area = db->get_U();
    header.right_most_pointer = 0;
}

BTreePage::BTreePage(DB* db): db(db), bytes(new uint8_t[db->get_page_size()]) { }

void BTreePage::recreate(uint32_t pg_n) {
    db->file.seekg((pg_n - 1) * db->get_page_size(), std::ios::beg);
    db->file.read(reinterpret_cast<char*>(bytes), db->get_page_size());

    uint16_t offset = 0;
    if (pg_n == 1) {
        is_first_page = true;
        offset = 100;
    }
    uint8_t page_type;
    offset += read_big_endian8(&page_type, bytes + offset);
    header.page_type = get_page_type(page_type);
    offset += read_big_endian16(&header.first_free_block, bytes + offset);
    offset += read_big_endian16(&header.num_of_cells, bytes + offset);
    offset += read_big_endian16(&header.start_of_cell_content_area, bytes + offset);
    offset += read_big_endian8(&header.num_of_fragmented_free_bytes_in_cell_content, bytes + offset);
    if (header.page_type == BTreePageType::InteriorIndexBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) {
        read_big_endian32(&header.right_most_pointer, bytes + offset);
    } else {
        header.right_most_pointer = 0;
    }
}

void BTreePage::recreate(BTreePageType page_type) {
    header.page_type = page_type;
    header.first_free_block = 0;
    header.num_of_cells = 0;
    header.start_of_cell_content_area = db->get_U();
    header.num_of_fragmented_free_bytes_in_cell_content = 0;
    header.right_most_pointer = 0;
}

BTreePage::~BTreePage() {
    delete[] bytes;
}

void BTreePage::write_header() {
    uint16_t offset = 0;
    if (is_first_page) {
        offset = 100;
    }
    uint8_t page_type = static_cast<uint8_t>(header.page_type);
    offset += write_big_endian8(page_type, bytes + offset);
    offset += write_big_endian16(header.first_free_block, bytes + offset);
    offset += write_big_endian16(header.num_of_cells, bytes + offset);
    offset += write_big_endian16(header.start_of_cell_content_area, bytes + offset);
    offset += write_big_endian8(header.num_of_fragmented_free_bytes_in_cell_content, bytes + offset);
    if (header.page_type == BTreePageType::InteriorIndexBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) {
        offset += write_big_endian32(header.right_most_pointer, bytes + offset);
    }
}

uint32_t BTreePage::get_right_most_pointer() {
    return header.right_most_pointer;
}

BTreePageType BTreePage::get_page_type(uint8_t page_type) {
    if (page_type == 0x02) {
        return BTreePageType::InteriorIndexBTreePage;
    }
    else if (page_type == 0x05) {
        return BTreePageType::InteriorTableBTreePage;
    }
    else if (page_type == 0x0a) {
        return BTreePageType::LeafIndexBTreePage;
    }
    else if (page_type == 0x0d) {
        return BTreePageType::LeafTableBTreePage;
    }
    return BTreePageType::Invalid;
}

uint8_t BTreePage::get_header_size() {
    return ((header.page_type == BTreePageType::InteriorIndexBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) ? 12 : 8) + (is_first_page ? 100 : 0);
}

uint64_t BTreePage::get_cell_rowid(uint16_t offset) { // 0 means something wrong
    uint64_t rowid, num_payload_bytes;
    uint32_t left_child_pointer;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            return 0;
            break;
        case BTreePageType::InteriorTableBTreePage:
            offset += read_big_endian32(&left_child_pointer, bytes + offset);
            offset += read_varint(&rowid, bytes + offset);
            return rowid;
            break;
        case BTreePageType::LeafIndexBTreePage:
            return 0;
            break;
        case BTreePageType::LeafTableBTreePage:
            offset += read_varint(&num_payload_bytes, bytes + offset);
            offset += read_varint(&rowid, bytes + offset);
            return rowid;
            break;
        default:
            return 0;
    }
}

uint32_t BTreePage::get_cell_left_child_pointer(uint16_t offset) { // 0 means something wrong
    uint32_t left_child_pointer;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            return 0;
            break;
        case BTreePageType::InteriorTableBTreePage:
            offset += read_big_endian32(&left_child_pointer, bytes + offset);
            return left_child_pointer;
            break;
        case BTreePageType::LeafIndexBTreePage:
            return 0;
            break;
        case BTreePageType::LeafTableBTreePage:
            return 0;
            break;
        default:
            return 0;
    }
}

uint32_t BTreePage::get_cell_first_overflow_page(uint16_t offset) {
    uint64_t rowid, num_payload_bytes, num_payload_bytes_in_page;
    uint32_t first_overflow_page;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            return 0;
            break;
        case BTreePageType::InteriorTableBTreePage:
            return 0;
            break;
        case BTreePageType::LeafIndexBTreePage:
            return 0;
            break;
        case BTreePageType::LeafTableBTreePage:
            offset += read_varint(&num_payload_bytes, bytes + offset);
            offset += read_varint(&rowid, bytes + offset);
            num_payload_bytes_in_page = compute_directly_stored_payload_size(num_payload_bytes);

            if (num_payload_bytes == num_payload_bytes_in_page) {
                return 0;
            }
            offset += read_big_endian32(&first_overflow_page, bytes + offset + num_payload_bytes_in_page);
            return first_overflow_page;
            break;
        default:
            return 0;
    }
}

uint64_t BTreePage::get_cell_payload_size(uint16_t offset) {
    uint64_t num_payload_bytes;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            return 0;
            break;
        case BTreePageType::InteriorTableBTreePage:
            return 0;
            break;
        case BTreePageType::LeafIndexBTreePage:
            return 0;
            break;
        case BTreePageType::LeafTableBTreePage:
            offset += read_varint(&num_payload_bytes, bytes + offset);
            return num_payload_bytes;
            break;
        default:
            return 0;
    }
}

void BTreePage::shift_cell_offsets_array(uint16_t idx) {
    for (int i = header.num_of_cells - 1; i > idx; --i) {
        uint16_t cell_content_offset = get_cell_content_offset(i - 1);
        write_cell_content_offset(i, cell_content_offset);
    }
}


bool BTreePage::compare_rowid(uint16_t idx, uint64_t id) {
    uint16_t cell_content_offset = get_cell_content_offset(idx);
    uint64_t rowid = get_cell_rowid(cell_content_offset);
    return id > rowid;
}

uint16_t BTreePage::lower_bound(uint64_t id) {
    uint16_t left = 0;
    uint16_t right = header.num_of_cells;

    while (left < right) {
        uint16_t mid = (right + left) / 2;
        if (compare_rowid(mid, id)) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

uint16_t BTreePage::compute_cell_size(uint64_t id, uint64_t P) {
    uint16_t cell_size = 0;
    if (header.page_type == BTreePageType::InteriorTableBTreePage || header.page_type == BTreePageType::InteriorIndexBTreePage) {
        cell_size += 4;
    }
    if (header.page_type == BTreePageType::LeafTableBTreePage || header.page_type == BTreePageType::LeafIndexBTreePage || header.page_type == BTreePageType::InteriorIndexBTreePage) {
        cell_size += get_n_bytes_in_varint(P);
        uint16_t directly_stored_payload = compute_directly_stored_payload_size(P);
        cell_size += directly_stored_payload;
        if (directly_stored_payload < P) {
            cell_size += 4;
        }
    }
    if (header.page_type == BTreePageType::LeafTableBTreePage || header.page_type == BTreePageType::InteriorTableBTreePage) {
        cell_size += get_n_bytes_in_varint(id);
    }
    return cell_size;
}

ReturnCodes BTreePage::insert_interior_cell(uint64_t id, uint16_t cell_offsets_idx, uint32_t left_child_pointer) {
    uint16_t cell_size = compute_cell_size(id);

    if (cell_size > compute_free_space()) {
        return ReturnCodes::NotEnoughSpaceToInsert;
    }

    header.start_of_cell_content_area -= cell_size;
    uint16_t offset = header.start_of_cell_content_area;
    header.num_of_cells++;

    shift_cell_offsets_array(cell_offsets_idx);
    write_cell_content_offset(cell_offsets_idx, offset);

    write_header();
    
    offset += write_big_endian32(left_child_pointer, bytes + offset);
    offset += write_varint(id, bytes + offset);
    return ReturnCodes::CellInserted;
}

ReturnCodes BTreePage::insert_leaf_cell(uint64_t id, uint16_t cell_offsets_idx, Payload* payload = nullptr) {
    uint16_t directly_stored_payload = compute_directly_stored_payload_size(payload->P);
    uint32_t first_overflow_page;
    uint16_t cell_size = compute_cell_size(id, payload->P);

    if (cell_size > compute_free_space()) {
        return ReturnCodes::NotEnoughSpaceToInsert;
    }

    header.start_of_cell_content_area -= cell_size;
    uint16_t offset = header.start_of_cell_content_area;
    header.num_of_cells++;

    shift_cell_offsets_array(cell_offsets_idx);
    write_cell_content_offset(cell_offsets_idx, offset);

    write_header();
    
    offset += write_varint(payload->P, bytes + offset);
    offset += write_varint(id, bytes + offset);
    std::memcpy(bytes + offset, payload->bytes, directly_stored_payload);
    offset += directly_stored_payload;

    if (directly_stored_payload < payload->P) {
        first_overflow_page = db->compute_database_size_in_pages() + 1;
        offset += write_big_endian32(first_overflow_page, bytes + offset);
    }

    if (directly_stored_payload < payload->P) {
        uint8_t* overflow_bytes = new uint8_t[db->get_page_size()];

        uint32_t n_overflow_pages = (payload->P - directly_stored_payload) / (db->get_U() - 4);
        if (((payload->P - directly_stored_payload) % (db->get_U() - 4)) != 0) {
            n_overflow_pages += 1;
        }

        db->header.database_size_in_pages += n_overflow_pages;

        for (uint32_t ovflw_pg_n = first_overflow_page; ovflw_pg_n < first_overflow_page + n_overflow_pages - 1; ++ovflw_pg_n) {
            std::memcpy(overflow_bytes + 4, payload->bytes + directly_stored_payload + (db->get_U() - 4) * (ovflw_pg_n - first_overflow_page), db->get_U() - 4);
            write_big_endian32(ovflw_pg_n + 1, overflow_bytes);
            db->write(ovflw_pg_n, overflow_bytes);
        }

        uint16_t last_ovflw_pg_size = (((payload->P - directly_stored_payload) % (db->get_U() - 4)) == 0) ? (db->get_U() - 4) : ((payload->P - directly_stored_payload) % (db->get_U() - 4));
        std::memcpy(overflow_bytes + 4, payload->bytes + directly_stored_payload + (db->get_U() - 4) * (n_overflow_pages - 1), last_ovflw_pg_size);
        write_big_endian32(0, overflow_bytes);

        db->write(first_overflow_page + n_overflow_pages - 1, overflow_bytes);

        delete[] overflow_bytes;
    }
    return ReturnCodes::CellInserted;
}

#define abs(x) ((x) < 0 ? -(x) : (x))

uint16_t BTreePage::get_split_index(uint16_t idx, uint16_t* sums, uint16_t* cell_sizes, uint16_t* cell_content_offsets) {
    uint16_t s = 0;

    for (uint16_t i = 0; i < header.num_of_cells; ++i) {
        uint16_t cell_content_offset = get_cell_content_offset(i);
        uint64_t cell_payload_size = get_cell_payload_size(cell_content_offset);
        uint64_t cell_rowid = get_cell_rowid(cell_content_offset);
        uint16_t cell_size = compute_cell_size(cell_rowid, cell_payload_size);

        if (i == idx) {
            s += cell_sizes[i];
            sums[i] = s;
        }

        cell_sizes[i + (i >= idx)] = cell_size;
        cell_content_offsets[i + (i >= idx)] = cell_content_offset;
        s += cell_size;
        sums[i + (i >= idx)] = s;
    }

    if (idx == header.num_of_cells) {
        s += cell_sizes[idx];
        sums[idx] = s;
    }

    uint16_t split_idx = 0;
    uint16_t S = sums[header.num_of_cells];
    uint16_t min_diff = S;
    for (uint16_t i = 0; i < header.num_of_cells + 1; ++i) {
        uint16_t diff = abs((S - sums[i]) - sums[i]);
        if (diff < min_diff) {
            min_diff = diff;
            split_idx = i;
        }
    }
    return split_idx;
}


// ----------------------- PRINTS ------------------------

void BTreePage::read_cell(uint16_t offset, Payload* p) {
    uint64_t num_payload_bytes, num_payload_bytes_in_page, rowid, bytes_to_read;
    uint32_t first_overflow_page;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            break;
        case BTreePageType::InteriorTableBTreePage:
            break;
        case BTreePageType::LeafIndexBTreePage:
            break;
        case BTreePageType::LeafTableBTreePage:
            offset += read_varint(&num_payload_bytes, bytes + offset);
            offset += read_varint(&rowid, bytes + offset);
            num_payload_bytes_in_page = compute_directly_stored_payload_size(num_payload_bytes);

            p->recreate(num_payload_bytes, rowid);

            std::memcpy(p->bytes, bytes + offset, num_payload_bytes_in_page);

            if (num_payload_bytes == num_payload_bytes_in_page) {
                return;
            }
            offset += read_big_endian32(&first_overflow_page, bytes + offset + num_payload_bytes_in_page);

            uint8_t buffer[4];
            offset = num_payload_bytes_in_page;

            while (first_overflow_page != 0) {
                db->file.seekg((db->get_page_size()) * (first_overflow_page - 1), std::ios::beg);
                db->file.read(reinterpret_cast<char*>(buffer), 4);
                read_big_endian32(&first_overflow_page, buffer);                
                if (first_overflow_page == 0 && ((num_payload_bytes - num_payload_bytes_in_page) % (db->get_U() - 4)) != 0) {
                    bytes_to_read = (num_payload_bytes - num_payload_bytes_in_page) % (db->get_U() - 4);
                } else {
                    bytes_to_read = db->get_U() - 4;
                }
                db->file.read(reinterpret_cast<char*>(p->bytes + offset), bytes_to_read);
                offset += bytes_to_read;
            }
            return;
        case BTreePageType::Invalid:
            break;
    }
    return;
}

void BTreePage::print_cell(uint16_t offset) {
    uint64_t num_payload_bytes, num_payload_bytes_in_page;
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            std::cout << "interior index b-tree page\n";
            break;
        case BTreePageType::InteriorTableBTreePage:
            std::cout << "--- interior table b-tree page ---\n";
            std::cout << "left child pointer: " << get_cell_left_child_pointer(offset) << "\n";
            std::cout << "integer key [rowid]: " << get_cell_rowid(offset) << "\n";
            break;
        case BTreePageType::LeafIndexBTreePage:
            std::cout << "leaf index b-tree page\n";
            break;
        case BTreePageType::LeafTableBTreePage:
            std::cout << "--- leaf table b-tree page cell ---\n";
            std::cout << "cell_offset: " << offset << "\n";
            num_payload_bytes = get_cell_payload_size(offset);
            std::cout << "total num of bytes in payload: " << get_cell_payload_size(offset) << "\n";
            std::cout << "rowid: " << get_cell_rowid(offset) << "\n";

            num_payload_bytes_in_page = compute_directly_stored_payload_size(num_payload_bytes);

            if (num_payload_bytes == num_payload_bytes_in_page) {
                std::cout << "payload [all in page]\n";
                break;
            }
            std::cout << "num bytes in page: " << num_payload_bytes_in_page << "\n";
            std::cout << "first_overflow_page: " << get_cell_first_overflow_page(offset) << "\n";
            break;
        default:
            std::cerr << "invalid btree page type value\n";
    }
}

void BTreePage::info() {
    std::cout << "Two variants of b-trees are used by SQLite"
    "'Table b-trees' use a 64-bit signed integer key and store all data in the leaves"
    "'Index b-trees' use arbitrary keys and store no data at all"

    "A b-tree page is either an interior page or a leaf page"
    "A leaf page contains keys and in the case of a table b-tree each key has associated data"
    "An interior page contains K keys together with K+1 pointers to child b-tree pages"
    "A 'pointer' in an interior b-tree page is just the 32-bit unsigned integer page number of the child page"

    "2 <= K <= as many keys as will fit on the page"

    "Large keys on index b-trees are split up into overflow pages"
    "so that no single key uses more than one fourth of the available storage space on the page"
    "and hence every internal page is able to store at least 4 keys"

    "Within an interior b-tree page"
    "each key and the pointer to its immediate left are combined into a structure called a 'cell'"
    "The right-most pointer is held separately"

    "It is possible (and in fact rather common) to have a complete b-tree"
    "that consists of a single page that is both a leaf and the root"

    "There is one table b-trees in the database file for each rowid table in the database schema"
    "including system tables such as sqlite_schema"
    "There is one index b-tree in the database file for each index in the schema"
    "including implied indexes created by uniquenessraints"

    "NO virtual tables and WITHOUT ROWID for now"

    "The b-tree corresponding to the sqlite_schema table is always a table b-tree and always has a root page of 1"
    "The sqlite_schema table contains the root page number for every other table and index in the database file"

    "Each entry in a table b-tree consists of a 64-bit signed integer key and up to 2147483647 bytes of arbitrary data"
    "The key of a table b-tree corresponds to the rowid of the SQL table that the b-tree implements"
    "Interior table b-trees hold only keys and pointers to children"
    "All data is contained in the table b-tree leaves"

    "Each entry in an index b-tree consists of an arbitrary key of up to 2147483647 bytes in length and no data"

    "Define the 'payload' of a cell to be the arbitrary length section of the cell"

    "When the size of payload for a cell exceeds a certain threshold (to be defined later)"
    "then only the first few bytes of the payload are stored on the b-tree page"
    "and the balance is stored in a linked list of content overflow pages"

    "A b-tree page is divided into regions in the following order:"

    "    1. The 100-uint8_t database file header (found on page 1 only)"
    "    2. The 8 or 12 uint8_t b-tree page header"
    "    3. The cell pointer array"
    "    4. Unallocated space"
    "    5. The cell content area"
    "    6. The reserved region.";
}

void BTreePage::print_header() {
    std::cout << "\n--- BTree page header ---\n\n";
    print_type();
    std::cout << "first free block: " << header.first_free_block << " [info: zero if there are no freeblocks]\n";
    std::cout << "number of cells on the page: " << header.num_of_cells << "\n";
    std::cout << "start of the cell content area: " << header.start_of_cell_content_area << " [info: zero value for this integer is interpreted as 65536]\n";
    std::cout << "number of fragmented free bytes within the cell content area: " << static_cast<int>(header.num_of_fragmented_free_bytes_in_cell_content) << " [info: in a well-formed b-tree page, the total number of bytes in fragments may not exceed 60]\n";
    std::cout << "right-most pointer: " << header.right_most_pointer << " [info: value appears in the header of interior b-tree pages only]\n";
    std::cout << "\n--- end BTree page header ---\n\n";
}

void BTreePage::print_type() {
    switch (header.page_type) {
        case BTreePageType::InteriorIndexBTreePage:
            std::cout << "interior index b-tree page\n";
            break;
        case BTreePageType::InteriorTableBTreePage:
            std::cout << "interior table b-tree page\n";
            break;
        case BTreePageType::LeafIndexBTreePage:
            std::cout << "leaf index b-tree page\n";
            break;
        case BTreePageType::LeafTableBTreePage:
            std::cout << "leaf table b-tree page\n";
            break;
        default:
            std::cerr << "invalid btree page type value\n";
    }
}

void BTreePage::print_cell_offsets_array(PrintCellFunc func) {
    int offset = get_header_size();
    uint16_t cell_content_offset;
    for (int i = 0; i < header.num_of_cells; ++i) {
        read_big_endian16(&cell_content_offset, bytes + offset + 2 * i);
        func(cell_content_offset);
    }
}

void BTreePage::print_cell_offsets_array() {
    int offset = get_header_size();
    uint16_t cell_content_offset;
    for (int i = 0; i < header.num_of_cells; ++i) {
        read_big_endian16(&cell_content_offset, bytes + offset + 2 * i);
        print_leaf_cell_rowid(cell_content_offset);
    }
}

void BTreePage::print_leaf_cell_rowid(uint16_t offset) {
    if (header.page_type != BTreePageType::LeafTableBTreePage) {
        return;
    }
    uint64_t rowid, num_payload_bytes;
    offset += read_varint(&num_payload_bytes, bytes + offset);
    offset += read_varint(&rowid, bytes + offset);
    std::cout << " [" << rowid << "] ";
}

void BTreePage::print() {
    print_header();
    int offset = get_header_size();
    uint16_t cell_content_offset;
    for (int i = 0; i < header.num_of_cells; ++i) {
        read_big_endian16(&cell_content_offset, bytes + offset + 2 * i);
        std::cout << "cell number: " << i << " cell content offset: " << static_cast<int>(cell_content_offset) << "\n";
        print_cell(cell_content_offset);
    }
    std::cout << "[info: SQLite strives to place cells as far toward the end of the b-tree page as it can, in order to leave space for future growth of the cell pointer array]\n";
}


uint64_t Payload::get_bytes_in_header(std::string& map) {
    uint64_t bytes_in_header; // including varint size itself
    uint64_t N = map.length() * 2 + 13;
    uint8_t bytes_in_serial_type_code = get_n_bytes_in_varint(N);
    bytes_in_header = get_n_bytes_in_varint_plus(bytes_in_serial_type_code + 1) + 1 + get_n_bytes_in_varint(N);
    return bytes_in_header;
}

uint64_t Payload::get_payload_size(std::string& map) {
    return get_bytes_in_header(map) + map.size();
}

Payload::Payload(std::string& map): P(get_payload_size(map)), bytes(new uint8_t[P]) {
    uint64_t offset = 0;
    uint64_t N = map.length() * 2 + 13;
    offset += write_varint(get_bytes_in_header(map), bytes + offset);
    offset += write_varint(0, bytes + offset);
    offset += write_varint(N, bytes + offset);
    for (char c : map) {
        offset += write_big_endian8(static_cast<uint8_t>(c), bytes + offset);
    }
}

Payload::Payload(uint64_t P): P(P), bytes(new uint8_t[P]) { }

Payload::~Payload() {
    delete[] bytes;
}

void Payload::recreate(uint64_t P, uint64_t rowid) {
    if (bytes != nullptr) {
        delete[] bytes;
    }
    this->P = P;
    bytes = new uint8_t[P];
    this->rowid = rowid;
}

int64_t Payload::get_integer_column(uint16_t column_idx) {
    uint64_t bytes_in_header, serial_type_code, content_size;
    int n_columns = 0;
    uint64_t offset = 0;
    offset += read_varint(&bytes_in_header, bytes + offset);

    uint64_t target_content_offset = bytes_in_header;

    while (offset < bytes_in_header) {
        offset += read_varint(&serial_type_code, bytes + offset);
        ++n_columns;
        if (n_columns == column_idx) {
            content_size = get_column_content_size(serial_type_code);
            break;
        }
        target_content_offset += get_column_content_size(serial_type_code);
    }

    if (n_columns != column_idx) {
        std::cout << "no column with index " << column_idx << "\n";
        return 0;
    }

    switch (content_size) {
        case 0: return rowid; break;
        case 1: return static_cast<int64_t>(read_int8(bytes + target_content_offset)); break;
        case 2: return static_cast<int64_t>(read_int16(bytes + target_content_offset)); break;
        case 3: return static_cast<int64_t>(read_int24(bytes + target_content_offset)); break;
        case 4: return static_cast<int64_t>(read_int32(bytes + target_content_offset)); break;
        case 6: return static_cast<int64_t>(read_int48(bytes + target_content_offset)); break;
        case 8: return static_cast<int64_t>(read_int64(bytes + target_content_offset)); break;
        default:
            std::cout << "Unsupported content size: " << content_size << std::endl;
            return 0;
    }
}

std::string Payload::get_text_column(uint16_t column_idx) {
    uint64_t bytes_in_header, serial_type_code, content_size;
    int n_columns = 0;
    uint64_t offset = 0;
    offset += read_varint(&bytes_in_header, bytes + offset);

    uint64_t target_content_offset = bytes_in_header;

    while (offset < bytes_in_header) {
        offset += read_varint(&serial_type_code, bytes + offset);
        ++n_columns;
        if (n_columns == column_idx) {
            content_size = get_column_content_size(serial_type_code);
            break;
        }
        target_content_offset += get_column_content_size(serial_type_code);
    }

    if (n_columns != column_idx) {
        std::cout << "no column with index " << column_idx << "\n";
        return std::string();
    }
    return std::string(reinterpret_cast<char*>(bytes + target_content_offset), content_size);
}

void Payload::print() {
    std::cout << "\n--- Payload Description ---\n\n";
    uint64_t bytes_in_header, serial_type_code, content_size; // including varint size itself

    int n_columns = 0;
    uint64_t offset = 0;
    offset += read_varint(&bytes_in_header, bytes + offset);

    std::vector<uint64_t> content_sizes;

    std::cout << "bytes in header: " << bytes_in_header << "\n";

    while (offset < bytes_in_header) {
        std::cout << "column: " << n_columns << "\n";
        offset += read_varint(&serial_type_code, bytes + offset);
        std::cout << "serial_type_code: " << serial_type_code << "\n";
        ++n_columns;
        content_size = print_serial_type_description(serial_type_code);
        content_sizes.push_back(content_size);
        std::cout << "\n";
    }

    for (uint64_t content_size : content_sizes) {
        std::cout << "CONTENT: ";
        if (content_size == 1) {
            std::cout << static_cast<int>(*bytes) << "\n";
        } else {
            print_bytes(bytes + offset, bytes + offset + content_size);
        }
        offset += content_size;
    }
}

uint64_t Payload::get_column_content_size(uint64_t serial_type) {
    if (serial_type <= 4) {
        return serial_type;
    } else if (serial_type == 5) {
        return 6;
    } else if (serial_type == 6 || serial_type == 7) {
        return 8;
    } else if (serial_type == 8 || serial_type == 9) {
        return 0;
    } else if (serial_type == 10 || serial_type == 11) {
        return 0; // these serial type codes will never appear in a well-formed database
    } else if (serial_type % 2 == 0) {
        return (serial_type - 12) / 2;
    } else {
        return (serial_type - 13) / 2;
    }
}

ColumnType Payload::get_column_type(uint64_t serial_type) {
    switch (serial_type) {
        case 0: return ColumnType::NULL_; break;
        case 1: return ColumnType::INT8; break;
        case 2: return ColumnType::BIG_ENDIAN_INT16; break;
        case 3: return ColumnType::BIG_ENDIAN_INT24; break;
        case 4: return ColumnType::BIG_ENDIAN_INT32; break;
        case 5: return ColumnType::BIG_ENDIAN_INT48; break;
        case 6: return ColumnType::BIG_ENDIAN_INT64; break;
        case 7: return ColumnType::BIG_ENDIAN_IEEE_754_2008_FLOAT64; break;
        case 8: return ColumnType::ZERO; break;
        case 9: return ColumnType::ONE; break;
        case 10: return ColumnType::RESERVED; break;
        case 11: return ColumnType::RESERVED; break;
        default:
            return (serial_type % 2 == 0) ? ColumnType::BLOB : ColumnType::STRING;
    }
}

uint64_t Payload::print_serial_type_description(uint64_t serial_type) {
    uint64_t content_size = get_column_content_size(serial_type);

    std::cout << "serial type: " << serial_type << "\n";
    std::cout << "content size: " << content_size << " bytes" << "\n";

    switch (serial_type) {
        case 0: std::cout << "value is a NULL." << "\n"; break;
        case 1: std::cout << "value is an 8-bit twos-complement integer." << "\n"; break;
        case 2: std::cout << "value is a big-endian 16-bit twos-complement integer." << "\n"; break;
        case 3: std::cout << "value is a big-endian 24-bit twos-complement integer." << "\n"; break;
        case 4: std::cout << "value is a big-endian 32-bit twos-complement integer." << "\n"; break;
        case 5: std::cout << "value is a big-endian 48-bit twos-complement integer." << "\n"; break;
        case 6: std::cout << "value is a big-endian 64-bit twos-complement integer." << "\n"; break;
        case 7: std::cout << "value is a big-endian IEEE 754-2008 64-bit floating point number." << "\n"; break;
        case 8: std::cout << "value is the integer 0." << "\n"; break;
        case 9: std::cout << "value is the integer 1." << "\n"; break;
        case 10:
        case 11:
            std::cout << "reserved for internal use." << "\n";
            std::cout << "these serial type codes will never appear in a well-formed database file," << "\n";
            std::cout << "but they might be used in transient and temporary database files." << "\n";
            std::cout << "the meanings of these codes can shift from one release of SQLite to the next." << "\n";
            break;
        default:
            if (serial_type % 2 == 0) {
                std::cout << "value is a BLOB that is " << content_size << " bytes in length." << "\n";
            } else {
                std::cout << "value is a string in the text encoding and " << content_size << " bytes in length." << "\n";
                std::cout << "the null terminator is not included." << "\n";
            }
    }
    return content_size;
}

void Payload::info() {
    std::cout << "The data for a table b-tree leaf page and the key of an index b-tree page was characterized above as an arbitrary sequence of bytes"
    "Mentioned one key being less than another, but did not define what 'less than' meant"

    "Payload is always in the 'record format'"
    "The record format specifies the number of columns, the datatype of each column, and the content of each column"
    "A record contains a header and a body, in that order"
    ;
}


Parser::Parser(Lexer& lex, DB* db, std::string& table_name): lex(lex), db(db), p(nullptr), table_name(table_name) {
    lex.scan();
}

void Parser::restart(size_t i) {
    lex.restart(i);
    lex.scan();
}

bool Parser::match(Tag expected) {
    if (lex.cur->tag == expected) {
        lex.scan();
        return true;
    } else {
        return false;
    }
}

void Parser::error(const std::string& message) {
    std::cerr << "parser error: " << message << "\n";
}

bool Parser::parse_values(Payload* p) {
    Token* token = lex.cur;
    if (token->tag != Tag::LEFT_BRACKET) {
        return false;
    }

    uint64_t rowid = 0;
    std::string text;
    uint64_t bytes_in_header = 0;
    uint64_t P = 0;
    size_t lexer_i = lex.i;

    for (auto pair : db->tables[table_name].columns) {
        uint16_t idx = pair.second;
        token = lex.scan();
        if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::TEXT) {
            if (token->tag != Tag::STRING_LITERAL) {
                return false;
            }
            text = static_cast<StringLiteral*>(token)->value;
            uint64_t serial_type = 2 * text.length() + 13;
            bytes_in_header += get_n_bytes_in_varint(serial_type);
            P += text.length();
        } else if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::INTEGER) {
            if (token->tag != Tag::INTEGER_LITERAL) {
                return false;
            }
            if (pair.first == "id") {
                rowid = static_cast<IntegerLiteral*>(token)->value;
                bytes_in_header += 1;
            } else {
                uint64_t serial_type = 6;
                bytes_in_header += get_n_bytes_in_varint(serial_type);
                P += 8;
            }
        }
        token = lex.scan();
        if (token->tag == Tag::RIGHT_BRACKET) {
            break;
        }
        if (token->tag != Tag::COMMA) {
            return false;
        }
    }

    bytes_in_header += get_n_bytes_in_varint_plus(bytes_in_header);
    P += bytes_in_header;

    p->recreate(P, rowid);

    uint64_t offset = 0;
    offset += write_varint(bytes_in_header, p->bytes + offset);

    lex.restart(lexer_i);

    for (auto pair : db->tables[table_name].columns) {
        uint16_t idx = pair.second;
        token = lex.scan();
        if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::TEXT) {
            text = static_cast<StringLiteral*>(token)->value;
            uint64_t serial_type = 2 * text.length() + 13;
            offset += write_varint(serial_type, p->bytes + offset);
        } else if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::INTEGER) {
            if (pair.first == "id") {
                uint64_t serial_type = 0;
                offset += write_varint(serial_type, p->bytes + offset);
            } else {
                uint64_t serial_type = 6;
                offset += write_varint(serial_type, p->bytes + offset);
            }
        }
        token = lex.scan();
        if (token->tag == Tag::RIGHT_BRACKET) {
            break;
        }
    }

    lex.restart(lexer_i);

    for (auto pair : db->tables[table_name].columns) {
        uint16_t idx = pair.second;
        token = lex.scan();
        if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::TEXT) {
            text = static_cast<StringLiteral*>(token)->value;
            for (char c : text) {
                offset += write_big_endian8(static_cast<uint8_t>(c), p->bytes + offset);
            }
        } else if (db->tables[table_name].columns_affinity[idx] == ColumnAffinity::INTEGER && pair.first != "id") {
            uint64_t n = static_cast<IntegerLiteral*>(token)->value;
            offset += write_int64(n, p->bytes + offset);
        }
        token = lex.scan();
        if (token->tag == Tag::RIGHT_BRACKET) {
            break;
        }
    }
    return true;
}

bool Parser::parse_where(Payload* p) {
    this->p = p;
    return parse_or();
}

bool Parser::parse_or() {
    bool res = parse_and();

    while (match(Tag::OR)) {
        res |= parse_and();
    }

    return res;
}

bool Parser::parse_and() {
    bool res = parse_comparison();

    while (match(Tag::AND)) {
        res &= parse_comparison();
    }

    return res;
}

bool Parser::parse_comparison() {
    std::string column;
    if (match(Tag::LEFT_BRACKET)) {
        bool res = parse_or();
        if (!match(Tag::RIGHT_BRACKET)) {
            return false;
        }
        return res;
    }

    if (lex.cur->tag != Tag::STRING_LITERAL) {
        error("expected string literal");
        return false;
    }

    column = static_cast<StringLiteral*>(lex.cur)->value;

    //std::cout << column << "\n";

    if (db->tables.count(table_name) == 0) {
        error("no table in db");
        return false;
    }

    if (db->tables[table_name].columns.count(column) == 0) {
        error("no column in table");
        return false;
    }

    if (db->tables[table_name].columns_affinity[db->tables[table_name].columns[column]] != ColumnAffinity::INTEGER) {
        error("not integer type");
        return false;
    }

    uint64_t v1 = p->get_integer_column(db->tables[table_name].columns[column] + 1);

    lex.scan();

    Tag t = lex.cur->tag;

    lex.scan();

    if (lex.cur->tag != Tag::INTEGER_LITERAL) {
        error("expected integer literal");
        return false;
    }

    int64_t v2 = static_cast<IntegerLiteral*>(lex.cur)->value;

    //std::cout << "comparing: " << v1 << " and " << v2 << "\n";

    lex.scan();

    return compare(t, v1, v2);
}