#include <iostream>
#include <vector>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "Packet.h"
#include "Protocol.h"

using namespace std;

int main() {
	Protocol obj;

	cout << obj.set_server(23333) << endl;

	while (1) {
		string filename;
		obj.rtp_accept(filename);
		cout << "Filename: " << filename << endl;

		string str = "";
		for (int i = 0; i < 1000; i++)
			str = str + "test";

		cout << obj.rtp_send(str) << endl;
		cout << "Done" << endl;
	}
}