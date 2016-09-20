#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifndef H_PACKET
#define H_PACKET

using namespace std;

#define BUFFSIZE 1024

class Packet {
public:
	int SEQ, ACK;
	bool SYN, FIN;
	int checksum;
	string content;

	Packet();
	Packet(char* buffer);

	bool is_valid();
	bool is_content_full();
	bool to_buffer(char* buffer);
};

#endif