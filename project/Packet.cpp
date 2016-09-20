#include "Packet.h"

Packet::Packet() {
	SEQ = ACK = checksum = 0;
	SYN = FIN = false;
	content = "";
}

Packet::Packet(char* buffer) {
	memcpy(&SEQ, buffer, sizeof(int));
	memcpy(&ACK, buffer + sizeof(int), sizeof(int));
	memcpy(&checksum, buffer + 2 * sizeof(int), sizeof(int));
	memcpy(&SYN, buffer + 3 * sizeof(int), sizeof(bool));
	memcpy(&FIN, buffer + 3 * sizeof(int) + sizeof(bool), sizeof(bool));

	int startByte = 3 * sizeof(int) + 2 * sizeof(bool);

	content = "";

	for (int i = startByte; i < BUFFSIZE; i++) {
		char ch;
		memcpy(&ch, buffer + i, sizeof(char));

		if (ch == '\0')
			break;
		else
			content = content + ch;
	}
}

bool Packet::to_buffer(char* buffer) {
	memset(buffer, 0, BUFFSIZE);
	memcpy(buffer, &SEQ, sizeof(int));
	memcpy(buffer + sizeof(int), &ACK, sizeof(int));
	memcpy(buffer + 2 * sizeof(int), &checksum, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), &SYN, sizeof(bool));
	memcpy(buffer + 3 * sizeof(int) + sizeof(bool), &FIN, sizeof(bool));

	int startByte = 3 * sizeof(int) + 2 * sizeof(bool);

	if (content.size() > BUFFSIZE - startByte) {
		cerr << "to_buffer error: packet's content is too large" << endl;
		return false;
	}

	for (int i = 0; i < content.size(); i++) {
		char ch = content[i];
		memcpy(buffer + startByte + i, &ch, sizeof(char));
	}

	if (content.size() < BUFFSIZE - startByte)
		memset(buffer + startByte + content.size(), 0, 1);

	return true;
}

bool Packet::is_valid() {
	//TODO 
	return true;
}

bool Packet::is_content_full() {
	int startByte = 3 * sizeof(int) + 2 * sizeof(bool);
	
	if (content.size() >= BUFFSIZE - startByte)
		return true;
	else
		return false;
}