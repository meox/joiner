CPP=g++

joiner: main.cpp
	${CPP} main.cpp -std=c++17 -O3 -Werror -Wall -o joiner -pthread

clean:
	rm joiner
	rm -rf *.o

all: joiner
