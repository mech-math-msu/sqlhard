#include "lexer.h"

bool compare(Tag cmp, uint64_t value1, uint64_t value2);

uint8_t read_big_endian8(uint8_t* v, const uint8_t* bytes);
uint8_t read_big_endian16(uint16_t* v, const uint8_t* bytes);
uint8_t read_big_endian32(uint32_t* v, const uint8_t* bytes);
uint8_t read_varint(uint64_t* v, const uint8_t* bytes);


uint8_t write_big_endian8(uint8_t v, uint8_t* bytes);
uint8_t write_big_endian16(uint16_t v, uint8_t* bytes);
uint8_t write_big_endian32(uint32_t v, uint8_t* bytes);
uint8_t write_varint(uint64_t v, uint8_t* bytes);

uint8_t get_n_bytes_in_varint(uint64_t v);


void print_binary(uint64_t v);
void print_binary(uint8_t v);
void print_bytes(uint8_t* start, uint8_t* end, char last = '\n');
void print_uint8_t(uint8_t v, char last = '\n');

int8_t read_int8(const uint8_t* bytes);
int16_t read_int16(const uint8_t* bytes);
int32_t read_int24(const uint8_t* bytes);
int32_t read_int32(const uint8_t* bytes);
int64_t read_int48(const uint8_t* bytes);
int64_t read_int64(const uint8_t* bytes);

uint8_t write_int64(uint64_t v, uint8_t* bytes);

bool compare(Tag cmp, uint64_t value1, uint64_t value2) {
    switch (cmp) {
        case Tag::EQUAL:
            return value1 == value2;
        case Tag::NOT_EQUAL:
            return value1 != value2;
        case Tag::GREATER_OR_EQUAL:
            return value1 >= value2;
        case Tag::LESS_OR_EQUAL:
            return value1 <= value2;
        case Tag::GREATER:
            return value1 > value2;
        case Tag::LESS:
            return value1 < value2;
        default:
            std::cerr << "invalid comparison operator\n";
            return false;
    }
}

int8_t read_int8(const uint8_t* bytes) {
    return static_cast<int8_t>(*bytes);
}

int16_t read_int16(const uint8_t* bytes) {
    int16_t value = (static_cast<int16_t>(bytes[0]) << 8) | static_cast<int16_t>(bytes[1]);
    return value;
}

int32_t read_int24(const uint8_t* bytes) {
    int32_t value = (static_cast<int32_t>(bytes[0]) << 16) |
                    (static_cast<int32_t>(bytes[1]) << 8) |
                    static_cast<int32_t>(bytes[2]);
    if (value & 0x800000) {
        value |= 0xFF000000;
    }
    return value;
}

int32_t read_int32(const uint8_t* bytes) {
    int32_t value = (static_cast<int32_t>(bytes[0]) << 24) |
                    (static_cast<int32_t>(bytes[1]) << 16) |
                    (static_cast<int32_t>(bytes[2]) << 8) |
                    static_cast<int32_t>(bytes[3]);
    return value;
}

int64_t read_int48(const uint8_t* bytes) {
    int64_t value = (static_cast<int64_t>(bytes[0]) << 40) |
                    (static_cast<int64_t>(bytes[1]) << 32) |
                    (static_cast<int64_t>(bytes[2]) << 24) |
                    (static_cast<int64_t>(bytes[3]) << 16) |
                    (static_cast<int64_t>(bytes[4]) << 8) |
                    static_cast<int64_t>(bytes[5]);
    if (value & 0x8000000000) {
        value |= 0xFFFF000000000000;
    }
    return value;
}

int64_t read_int64(const uint8_t* bytes) {
    int64_t value = (static_cast<int64_t>(bytes[0]) << 56) |
                    (static_cast<int64_t>(bytes[1]) << 48) |
                    (static_cast<int64_t>(bytes[2]) << 40) |
                    (static_cast<int64_t>(bytes[3]) << 32) |
                    (static_cast<int64_t>(bytes[4]) << 24) |
                    (static_cast<int64_t>(bytes[5]) << 16) |
                    (static_cast<int64_t>(bytes[6]) << 8) |
                    static_cast<int64_t>(bytes[7]);
    return value;
}

uint8_t write_int64(uint64_t v, uint8_t* bytes) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&v);
    std::memcpy(bytes, ptr, sizeof(uint64_t));
    return sizeof(uint64_t);
}

void print_uint8_t(uint8_t v, char last) {
    std::cout << static_cast<int>(v) << last;
}

void print_bytes(uint8_t* start, uint8_t* end, char last) {
    while (start != end) {
        std::cout << *start;
        ++start;
    }
    std::cout << last;
}

void print_binary(uint64_t v) {
    for (int i = 63; i >= 0; --i) {
        uint64_t mask = 1ULL << i;
        std::cout << ((v & mask) ? '1' : '0');

        if (i % 8 == 0) {
            std::cout << ' ';
        }
    }

    std::cout << "\n";
}

void print_binary(uint8_t v) {
    for (int i = 7; i >= 0; --i) {
        uint8_t mask = 1 << i;
        std::cout << ((v & mask) ? '1' : '0');
    }

    std::cout << "\n";
}

// The header size varint and serial type varints will usually consist of a single byte
// The serial type varints for large strings and BLOBs might extend to two or three byte varints,
// but that is the exception rather than the rule
// The varint format is very efficient at coding the record header
// Thats why they have this scary code

/*
** The variable-length integer encoding is as follows:
**
** KEY:
**         A = 0xxxxxxx    7 bits of data and one flag bit
**         B = 1xxxxxxx    7 bits of data and one flag bit
**         C = xxxxxxxx    8 bits of data
**
**  7 bits - A
** 14 bits - BA
** 21 bits - BBA
** 28 bits - BBBA
** 35 bits - BBBBA
** 42 bits - BBBBBA
** 49 bits - BBBBBBA
** 56 bits - BBBBBBBA
** 64 bits - BBBBBBBBC
*/

uint8_t read_varint(uint64_t* v, const uint8_t* bytes) {
    *v = 0;
    uint8_t offset = 0;

    for (; (*(bytes + offset) & 0b10000000) && offset <= 8; ++offset) {
        *v |= static_cast<uint64_t>(*(bytes + offset) & 0b01111111);
        *v <<= 7;
    }
    if (offset == 8) {
        *v |= static_cast<uint64_t>(*(bytes + offset));
        return 9;
    }
    *v |= static_cast<uint64_t>(*(bytes + offset) & 0b01111111);
    return offset + 1;
}

uint8_t write_varint(uint64_t v, uint8_t* bytes) {
    if (v <= 0x7f) {
        bytes[0] = v & 0x7f;
        return 1;
    }
    if (v <= 0x3fff) {
        bytes[0] = ((v >> 7) & 0x7f) | 0x80;
        bytes[1] = v & 0x7f;
        return 2;
    }

    int i, j, n;
    uint8_t buf[10];
    if (v & (((uint64_t) 0xff000000) << 32)) {
        bytes[8] = (uint8_t) v;
        v >>= 8;
        for (i = 7; i >= 0; --i) {
            bytes[i] = (uint8_t) ((v & 0x7f) | 0x80);
            v >>= 7;
        }
        return 9;
    }

    n = 0;
    do {
        buf[n++] = (uint8_t)((v & 0x7f) | 0x80);
        v >>= 7;
    } while (v != 0);
    buf[0] &= 0x7f;

    //assert (n <= 9);

    for (i = 0, j = n - 1; j >= 0; j--, i++) {
        bytes[i] = buf[j];
    }
    return n;
}

uint8_t get_n_bytes_in_varint_plus(uint64_t v) {
    if (v + 1 <= 0x7f) {
        return 1;
    }
    if (v + 2 <= 0x3fff) {
        return 2;
    }
    if (v + 3 <= 0x1fffff) {
        return 3;
    }
    if (v + 4 <= 0xfffffff) {
        return 4;
    }
    if (v + 5 <= 0x7ffffffff) {
        return 5;
    }
    if (v + 6 <= 0x3ffffffffff) {
        return 6;
    }
    if (v + 7 <= 0x1ffffffffffff) {
        return 7;
    }
    if (v + 8 <= 0xfffffffffffffff) {
        return 8;
    }
    return 9; // Maximum of 9 bytes for 64-bit integers
}

uint8_t get_n_bytes_in_varint(uint64_t v) {
    if (v <= 0x7f) {
        return 1;
    }
    if (v <= 0x3fff) {
        return 2;
    }
    if (v <= 0x1fffff) {
        return 3;
    }
    if (v <= 0xfffffff) {
        return 4;
    }
    if (v <= 0x7ffffffff) {
        return 5;
    }
    if (v <= 0x3ffffffffff) {
        return 6;
    }
    if (v <= 0x1ffffffffffff) {
        return 7;
    }
    if (v <= 0xfffffffffffffff) {
        return 8;
    }
    return 9; // Maximum of 9 bytes for 64-bit integers
}

// uint8_t get_n_bytes_in_varint(uint64_t v) {
//     if (v <= 0x7f) {
//         return 1;
//     }
//     if (v <= 0x3fff) {
//         return 2;
//     }

//     int i, n;
//     uint8_t buf[10];
//     if (v & (((uint64_t) 0xff000000) << 32)) {
//         v >>= 8;
//         for (i = 7; i >= 0; --i) {
//             v >>= 7;
//         }
//         return 9;
//     }

//     n = 0;
//     do {
//         buf[n++] = (uint8_t)((v & 0x7f) | 0x80);
//         v >>= 7;
//     } while (v != 0);
//     buf[0] &= 0x7f;
//     return n;
// }

uint8_t read_big_endian8(uint8_t* v, const uint8_t* bytes) {
    *v = 0;
    *v = bytes[0];
    return 1;
}

uint8_t read_big_endian16(uint16_t* v, const uint8_t* bytes) {
    *v = 0;
    *v |= static_cast<uint16_t>(bytes[0]) << 8;
    *v |= static_cast<uint16_t>(bytes[1]);
    return 2;
}

uint8_t read_big_endian32(uint32_t* v, const uint8_t* bytes) {
    *v = 0;
    *v |= static_cast<uint32_t>(bytes[0]) << 24;
    *v |= static_cast<uint32_t>(bytes[1]) << 16;
    *v |= static_cast<uint32_t>(bytes[2]) << 8;
    *v |= static_cast<uint32_t>(bytes[3]);
    return 4;
}

uint8_t write_big_endian8(uint8_t v, uint8_t* bytes) {
    bytes[0] = v;
    return 1;
}

uint8_t write_big_endian16(uint16_t v, uint8_t* bytes) {
    bytes[0] = static_cast<uint8_t>((v >> 8) & 0xFF);
    bytes[1] = static_cast<uint8_t>(v & 0xFF);
    return 2;
}

uint8_t write_big_endian32(uint32_t v, uint8_t* bytes) {
    bytes[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
    bytes[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
    bytes[2] = static_cast<uint8_t>((v >> 8) & 0xFF);
    bytes[3] = static_cast<uint8_t>(v & 0xFF);
    return 4;
}