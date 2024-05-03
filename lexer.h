#include <unordered_map>
#include <fstream>

bool is_digit(char);
bool is_qoute(char);
bool is_letter(char);
char to_upper(char);
bool is_letter_or_digit(char);
int to_digit(char);
static char EOF_CHAR = 26;

enum class Tag {
    INTEGER_LITERAL,
    REAL_LITERAL,
    STRING_LITERAL,
    TYPE_TEXT,
    TYPE_INTEGER,
    TYPE_NUMERIC,
    TYPE_BLOB,
    TYPE_REAL,
    CREATE,
    TABLE,
    INSERT,
    INTO,
    VALUES,
    SELECT,
    FROM,
    WHERE,
    AND,
    OR,
    LESS,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
    NOT_EQUAL,
    EQUAL,
    UNARY_MINUS,
    LEFT_BRACKET,
    RIGHT_BRACKET,
    EOF_TOKEN,
    ERROR,
    COMMA,
    ALL
};

static std::unordered_map<std::string, Tag> TAG_MAP {
    {"TEXT", Tag::TYPE_TEXT},
    {"BLOB", Tag::TYPE_BLOB},
    {"INTEGER", Tag::TYPE_INTEGER},
    {"NUMERIC", Tag::TYPE_NUMERIC},
    {"REAL", Tag::TYPE_REAL},
    {"CREATE", Tag::CREATE},
    {"TABLE", Tag::TABLE},
    {"INSERT", Tag::INSERT},
    {"INTO", Tag::INTO},
    {"VALUES", Tag::VALUES},
    {"SELECT", Tag::SELECT},
    {"FROM", Tag::FROM},
    {"WHERE", Tag::WHERE},
    {"AND", Tag::AND},
    {"OR", Tag::OR}
};

struct Token {
    Tag tag;
    Token(): tag(Tag::EOF_TOKEN) { }
    Token(Tag t): tag(t) { }
    Token(const Token& other): tag(other.tag) { }
    virtual ~Token() { }
    Token& operator=(const Token& other) {
        tag = other.tag;
        return *this;
    }
};

struct IntegerLiteral: Token {
    int64_t value;
    IntegerLiteral(int64_t v): Token(Tag::INTEGER_LITERAL), value(v) {}
};

struct RealLiteral: Token {
    double value;
    RealLiteral(double v): Token(Tag::REAL_LITERAL), value(v) {}
};

struct StringLiteral: Token {
    std::string value;
    StringLiteral(const std::string& s): Token(Tag::STRING_LITERAL), value(s) {}
};

std::string tag_to_string(Tag tag) {
    switch (tag) {
        case Tag::INTEGER_LITERAL:
            return "INTEGER_LITERAL";
        case Tag::REAL_LITERAL:
            return "REAL_LITERAL";
        case Tag::STRING_LITERAL:
            return "STRING_LITERAL";
        case Tag::TYPE_TEXT:
            return "TYPE_TEXT";
        case Tag::TYPE_INTEGER:
            return "TYPE_INTEGER";
        case Tag::TYPE_NUMERIC:
            return "TYPE_NUMERIC";
        case Tag::TYPE_BLOB:
            return "TYPE_BLOB";
        case Tag::TYPE_REAL:
            return "TYPE_REAL";
        case Tag::CREATE:
            return "CREATE";
        case Tag::TABLE:
            return "TABLE";
        case Tag::INSERT:
            return "INSERT";
        case Tag::INTO:
            return "INTO";
        case Tag::VALUES:
            return "VALUES";
        case Tag::SELECT:
            return "SELECT";
        case Tag::FROM:
            return "FROM";
        case Tag::WHERE:
            return "WHERE";
        case Tag::AND:
            return "AND";
        case Tag::OR:
            return "OR";
        case Tag::LESS:
            return "LESS";
        case Tag::LESS_OR_EQUAL:
            return "LESS_OR_EQUAL";
        case Tag::GREATER:
            return "GREATER";
        case Tag::GREATER_OR_EQUAL:
            return "GREATER_OR_EQUAL";
        case Tag::NOT_EQUAL:
            return "NOT_EQUAL";
        case Tag::EQUAL:
            return "EQUAL";
        case Tag::UNARY_MINUS:
            return "UNARY_MINUS";
        case Tag::LEFT_BRACKET:
            return "LEFT_BRACKET";
        case Tag::RIGHT_BRACKET:
            return "RIGHT_BRACKET";
        case Tag::ERROR:
            return "ERROR";
        case Tag::COMMA:
            return "COMMA";
        case Tag::EOF_TOKEN:
            return "EOF_CHAR";
        case Tag::ALL:
            return "ALL";
    }
    return "BAD_TAG";
}

void print_tag(Tag& tag) {
    std::cout << tag_to_string(tag) << "\n";
}

struct Lexer {
    int line = 1;
    size_t i = 0;
    char peek = ' ';
    const std::string& s;
    Token* cur = nullptr;

    Lexer(const std::string& s): s(s) { }
    ~Lexer() {
        if (cur != nullptr) {
            delete cur;
        }
    }
    void restart(size_t i = 0) {
        if (cur != nullptr) {
            delete cur;
            cur = nullptr;
        }
        this->i = i;
        peek = ' ';
    }
    void next_char() {
        peek = s[i];
        ++i;
        if (i == s.length() + 1) { peek = EOF_CHAR; }
    }
    bool next_char_and_compare(char c) {
        next_char();
        return peek == c;
    }
    Token* scan() {
        if (cur != nullptr) {
            delete cur;
            cur = nullptr;
        }
        for (;; next_char()) {
            if ( peek == '\t' || peek == ' ') continue;
            else if ( peek == '\n' ) ++line;
            else break;
        }

        if (is_digit(peek)) {
            int64_t n = 0;
            do {
                n = 10 * n + to_digit(peek);
                next_char();
            } while (is_digit(peek));
            cur = new IntegerLiteral(n);
            return cur;
        }

        std::string s;
        if (is_letter(peek)) {
            s = "";
            do {
                s += peek;
                next_char();
            } while (is_letter_or_digit(peek));

            try {
                std::string keyword = "";
                for (char c : s) {
                    keyword += to_upper(c);
                }
                cur = new Token(TAG_MAP.at(keyword));
                return cur;
            } catch (const std::out_of_range&) {
                cur = new StringLiteral(s);
                return cur;
            }
        }

        if (is_qoute(peek)) {
            s = "";
            next_char();
            while (!is_qoute(peek)) {
                s += peek;
                next_char();
            }
            next_char();
            cur = new StringLiteral(s);
            return cur;
        }

        if (peek == ',') {
            next_char();
            cur = new Token(Tag::COMMA);
            return cur;
        }

        if (peek == '*') {
            next_char();
            cur = new Token(Tag::ALL);
            return cur;
        }

        if (peek == '=') {
            next_char();
            cur = new Token(Tag::EQUAL);
            return cur;
        }

        if (peek == '(') {
            next_char();
            cur = new Token(Tag::LEFT_BRACKET);
            return cur;
        }

        if (peek == ')') {
            next_char();
            cur = new Token(Tag::RIGHT_BRACKET);
            return cur;
        }

        if (peek == '!') {
            if (next_char_and_compare('=')) {
                next_char();
                cur = new Token(Tag::NOT_EQUAL);
                return cur;
            }
            next_char();
            cur = new Token(Tag::ERROR);
            return cur;
        }

        if (peek == '<') {
            if (next_char_and_compare('=')) {
                next_char();
                cur = new Token(Tag::LESS_OR_EQUAL);
                return cur;
            }
            next_char();
            cur = new Token(Tag::LESS);
            return cur;
        }

        if (peek == '>') {
            if (next_char_and_compare('=')) {
                next_char();
                cur = new Token(Tag::GREATER_OR_EQUAL);
                return cur;
            }
            next_char();
            cur = new Token(Tag::GREATER);
            return cur;
        }

        if (peek == EOF_CHAR) {
            cur = new Token(Tag::EOF_TOKEN);
            return cur;
        }

        cur = new Token(Tag::ERROR);
        return cur;
    }
};

bool is_qoute(char c) {
    return c == 39;
}

bool is_digit(char c) {
    return 48 <= c && c <= 57;
}

bool is_letter(char c) {
    return (65 <= c && c <= 95) || (97 <= c && c <= 122);
}

char to_upper(char c) {
    return (97 <= c && c <= 122) ? c - (97 - 65) : c;
}

int to_digit(char c) {
    return c - 48;
}

bool is_letter_or_digit(char c) {
    return is_digit(c) || is_letter(c);
}
