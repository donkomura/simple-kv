CXX     := g++
CFLAGS  := `pkg-config --cflags libpmemobj++`
LDFLAGS := `pkg-config --libs libpmemobj++`
FLAGS   := -O -Wall -g -std=c++11
RM      := rm

.PHONY: all clean

all: simplekv

simplekv: simplekv.cpp
	$(CXX) $< -o $@ $(FLAGS) $(CFLAGS) $(LDFLAGS)

clean:
	$(RM) simplekv

