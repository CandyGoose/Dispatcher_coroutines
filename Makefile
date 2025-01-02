SRC_DIR   = source
BUILD_DIR = build
OBJ_DIR   = $(BUILD_DIR)/obj
BIN_DIR   = $(BUILD_DIR)/bin

COMPILER           ?= clang
OPTIMIZATION_LEVEL ?= -O3
SANITIZERS         ?=

CC        = $(COMPILER)

CFLAGS    = -g -I $(SRC_DIR)
CFLAGS   += -Wall -Werror
CFLAGS   += -fno-omit-frame-pointer
CFLAGS   += $(OPTIMIZATION_LEVEL)
CFLAGS   += $(SANITIZERS)

LDFLAGS   = $(CFLAGS)

SRCS     := $(shell find source -iname '*.c') $(shell find source -iname '*.S')

OBJS     := $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(SRCS))
OBJS     := $(patsubst $(SRC_DIR)/%.S, $(OBJ_DIR)/%.o, $(OBJS))

TARGET    = app

run: $(BIN_DIR)/$(TARGET)
	./$(BIN_DIR)/$(TARGET)

compile: clean $(BIN_DIR)/$(TARGET)

clean:
	rm -rf $(BUILD_DIR)
	rm -rf ./**/*.o

$(BIN_DIR)/$(TARGET): $(BIN_DIR) $(OBJ_DIR) $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	mkdir -p $(shell dirname $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.S
	mkdir -p $(shell dirname $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR): $(BUILD_DIR)
	mkdir -p $(OBJ_DIR)

$(BIN_DIR): $(BUILD_DIR)
	mkdir -p $(BIN_DIR)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

check-style:
	find source -iname '*.h' -o -iname '*.c' \
	| xargs clang-tidy -p compile_commands.json

check-format:
	find source -iname '*.h' -o -iname '*.c' \
	| xargs clang-format -Werror --dry-run --fallback-style=Google --verbose

fix-format:
	find source -iname '*.h' -o -iname '*.c' \
	| xargs clang-format -i --fallback-style=Google --verbose

info:
	$(info CC        = $(CC))
	$(info CFLAGS    = $(CFLAGS))
	$(info LDFLAGS   = $(LDFLAGS))
	$(info SRC_DIR   = $(SRC_DIR))
	$(info BUILD_DIR = $(BUILD_DIR))
	$(info SRCS      = $(SRCS))
	$(info OBJS      = $(OBJS))
