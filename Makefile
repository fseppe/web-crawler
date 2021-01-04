CC := g++
SRCDIR := src
OBJDIR := build

SRCEXT := cpp
SOURCES := $(shell find $(SRCDIR) -type f -name '*.$(SRCEXT)')
OBJECTS := $(patsubst $(SRCDIR)/%,$(OBJDIR)/%,$(SOURCES:.$(SRCEXT)=.o))

CFLAGS := -g -Wall -std=c++11
INC := -Iinclude -lpthread -I$(CHILKAT_PATH)/include -L$(CHILKAT_PATH)/lib -lchilkat-9.5.0

MAIN := main.cpp
MAIN_OBJ := crawler

TEST := testes.cpp
TEST_OBJ := teste

$(OBJDIR)/%.o: $(SRCDIR)/%.$(SRCEXT)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

main: $(OBJECTS)
	$(CC) $(MAIN) $(INC) $(CFLAGS) $^ -o $(MAIN_OBJ)

test: $(OBJECTS)
	$(CC) $(TEST) $(INC) $(CFLAGS) $^ -o $(TEST_OBJ)

all: main test

clean:
	rm $(MAIN_OBJ)
	rm $(TEST_OBJ)
	rm $(OBJDIR)/*
