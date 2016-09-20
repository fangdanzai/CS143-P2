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

	cout << obj.set_client("127.0.0.1", 23333) << endl;

	obj.rtp_connect("gan.cpp");

	string message = "";
	cout << obj.rtp_read(message) << endl;
	cout << message << endl;
}