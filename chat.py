import sys
import os
import json
import uuid
import logging
from queue import  Queue
import threading
import socket
import base64
from datetime import datetime
from os.path import join, dirname, realpath

class RealmThreadCommunication(threading.Thread):
    def __init__(self, chats, realm_dest_address, realm_dest_port):
        self.chats = chats
        self.chat = {}
        self.realm_dest_address = realm_dest_address
        self.realm_dest_port = realm_dest_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.realm_dest_address, self.realm_dest_port))
        threading.Thread.__init__(self)

    def sendstring(self, string):
        try:
            self.sock.sendall(string.encode())
            receivedmsg = ""
            while True:
                data = self.sock.recv(1024)
                print("diterima dari server", data)
                if (data):
                    receivedmsg = "{}{}" . format(receivedmsg, data.decode())  #data harus didecode agar dapat di operasikan dalam bentuk string
                    if receivedmsg[-4:]=='\r\n\r\n':
                        print("end of string")
                        return json.loads(receivedmsg)
        except:
            self.sock.close()
            return { 'status' : 'ERROR', 'message' : 'Gagal'}
    
    def put(self, message):
        dest = message['msg_to']
        try:
            self.chat[dest].put(message)
        except KeyError:
            self.chat[dest]=Queue()
            self.chat[dest].put(message)

class Chat:
	def __init__(self):
		self.sessions={}
		self.users = {}
		self.realms = {}
		self.users['messi']={ 'nama': 'Lionel Messi', 'negara': 'Argentina', 'password': 'surabaya', 'incoming' : {}, 'outgoing': {}}
		self.users['henderson']={ 'nama': 'Jordan Henderson', 'negara': 'Inggris', 'password': 'surabaya', 'incoming': {}, 'outgoing': {}}
		self.users['lineker']={ 'nama': 'Gary Lineker', 'negara': 'Inggris', 'password': 'surabaya','incoming': {}, 'outgoing':{}}
	def proses(self,data):
		j=data.split(" ")
		try:
			command=j[0].strip()
			if (command=='auth'):
				username=j[1].strip()
				password=j[2].strip()
				logging.warning("AUTH: auth {} {}" . format(username,password))
				return self.autentikasi_user(username,password)
			# Satu server
			elif (command=='send'):
				sessionid = j[1].strip()
				usernameto = j[2].strip()
				message=""
				for w in j[3:]:
					message="{} {}" . format(message,w)
				usernamefrom = self.sessions[sessionid]['username']
				logging.warning("SEND: session {} send message from {} to {}" . format(sessionid, usernamefrom,usernameto))
				return self.send_message(sessionid,usernamefrom,usernameto,message)
			elif (command=='inbox'):
				sessionid = j[1].strip()
				username = self.sessions[sessionid]['username']
				logging.warning("INBOX: {}" . format(sessionid))
				return self.get_inbox(username)
			elif (command=='sendgroup'):
				sessionid = j[1].strip()
				usernamesto = j[2].strip().split(',')
				message=""
				for w in j[3:]:
					message="{} {}" . format(message,w)
				usernamefrom = self.sessions[sessionid]['username']
				logging.warning("SEND: session {} send message from {} to {}" . format(sessionid, usernamefrom,usernamesto))
				return self.send_group_message(sessionid,usernamefrom,usernamesto,message)
			elif (command == 'getrealminbox'):
				session_id = j[1].strip()
				realm_id = j[2].strip()
				username = self.sessions[session_id]['username']
				logging.warning("INBOX: {} in realm {}".format(session_id, realm_id))
				return self.inbox_realm(session_id, username, realm_id, data)
			# Beda Server
			elif (command=='addrealm'):
				realm_id = j[1].strip()
				realm_dest_address = j[2].strip()
				realm_dest_port = int(j[3].strip())
				return self.add_realm(realm_id, realm_dest_address, realm_dest_port, data)
			elif (command=='recvrealm'):
				realm_id = j[1].strip()
				realm_dest_address = j[2].strip()
				realm_dest_port = int(j[3].strip())
				return self.recv_realm(realm_id, realm_dest_address, realm_dest_port, data)
			elif (command == 'sendprivaterealm'):
				sessionid = j[1].strip()
				realm_id = j[2].strip()
				usernameto = j[3].strip()
				message = ""
				for w in j[4:]:
					message = "{} {}".format(message, w)
				usernamefrom = self.sessions[sessionid]['username']
				logging.warning("SENDPRIVATEREALM: session {} send message from {} to {} in realm {}".format(sessionid, usernamefrom, usernameto, realm_id))
				return self.send_realm_message(sessionid, realm_id, usernamefrom, usernameto, message, data)
			elif (command == 'recvrealmprivatemsg'):
				usernamefrom = j[1].strip()
				realm_id = j[2].strip()
				usernameto = j[3].strip()
				message = ""
				for w in j[4:]:
					message = "{} {}".format(message, w)
				logging.warning("RECVREALMPRIVATEMSG: recieve message from {} to {} in realm {}".format( usernamefrom, usernameto, realm_id))
				return self.recv_realm_message(realm_id, usernamefrom, usernameto, message, data)
			elif (command == 'sendgrouprealm'):
				sessionid = j[1].strip()
				realm_id = j[2].strip()
				usernamesto = j[3].strip().split(',')
				message = ""
				for w in j[4:]:
					message = "{} {}".format(message, w)
				usernamefrom = self.sessions[sessionid]['username']
				logging.warning("SENDGROUPREALM: session {} send message from {} to {} in realm {}".format(sessionid, usernamefrom, usernamesto, realm_id))
				return self.send_group_realm_message(sessionid, realm_id, usernamefrom,usernamesto, message,data)
			elif (command == 'recvrealmgroupmsg'):
				usernamefrom = j[1].strip()
				realm_id = j[2].strip()
				usernamesto = j[3].strip().split(',')
				message = ""
				for w in j[4:]:
					message = "{} {}".format(message, w) 
				logging.warning("RECVGROUPREALM: send message from {} to {} in realm {}".format(usernamefrom, usernamesto, realm_id))
				return self.recv_group_realm_message(realm_id, usernamefrom,usernamesto, message,data)
			else:
				return {'status': 'ERROR', 'message': '**Protocol Tidak Benar'}
		except KeyError:
			return { 'status': 'ERROR', 'message' : 'Informasi tidak ditemukan'}
		except IndexError:
			return {'status': 'ERROR', 'message': '--Protocol Tidak Benar'}
            
	# -----------------------------End Beda Server---------------------------------------------------------------

	def autentikasi_user(self,username,password):
		if (username not in self.users):
			return { 'status': 'ERROR', 'message': 'User Tidak Ada' }
		if (self.users[username]['password']!= password):
			return { 'status': 'ERROR', 'message': 'Password Salah' }
		tokenid = str(uuid.uuid4()) 
		self.sessions[tokenid]={ 'username': username, 'userdetail':self.users[username]}
		return { 'status': 'OK', 'tokenid': tokenid }
	
	def register_user(self, username, password, nama, negara):
		if username in self.users:
			return {"status": "ERROR", "message": "User Sudah Ada"}
		nama = nama.replace("_", " ")
		self.users[username] = {
            "nama": nama,
            "negara": negara,
            "password": password,
            "incoming": {},
            "outgoing": {},
        }
		tokenid = str(uuid.uuid4())
		self.sessions[tokenid] = {
            "username": username,
            "userdetail": self.users[username],
        }
		return {"status": "OK", "tokenid": tokenid}
	def get_user(self,username):
		if (username not in self.users):
			return False
		return self.users[username]
	def send_message(self,sessionid,username_from,username_dest,message):
		if (sessionid not in self.sessions):
			return {'status': 'ERROR', 'message': 'Session Tidak Ditemukan'}
		s_fr = self.get_user(username_from)
		s_to = self.get_user(username_dest)
		
		if (s_fr==False or s_to==False):
			return {'status': 'ERROR', 'message': 'User Tidak Ditemukan'}

		message = { 'msg_from': s_fr['nama'], 'msg_to': s_to['nama'], 'msg': message }
		outqueue_sender = s_fr['outgoing']
		inqueue_receiver = s_to['incoming']
		try:	
			outqueue_sender[username_from].put(message)
		except KeyError:
			outqueue_sender[username_from]=Queue()
			outqueue_sender[username_from].put(message)
		try:
			inqueue_receiver[username_from].put(message)
		except KeyError:
			inqueue_receiver[username_from]=Queue()
			inqueue_receiver[username_from].put(message)
		return {'status': 'OK', 'message': 'Message Sent'}
	def send_group_message(self, sessionid, username_from, usernames_dest, message):
		if (sessionid not in self.sessions):
			return {'status': 'ERROR', 'message': 'Session Tidak Ditemukan'}
		s_fr = self.get_user(username_from)
		if s_fr is False:
			return {'status': 'ERROR', 'message': 'User Tidak Ditemukan'}
		for username_dest in usernames_dest:
			s_to = self.get_user(username_dest)
			if s_to is False:
				continue
			message = {'msg_from': s_fr['nama'], 'msg_to': s_to['nama'], 'msg': message}
			outqueue_sender = s_fr['outgoing']
			inqueue_receiver = s_to['incoming']
			
			try:
				outqueue_sender[username_from].put(message)
			except KeyError:
				outqueue_sender[username_from]=Queue()
				outqueue_sender[username_from].put(message)
			try:
				inqueue_receiver[username_from].put(message)
			except KeyError:
				inqueue_receiver[username_from]=Queue()
				inqueue_receiver[username_from].put(message)
		
		return {'status': 'OK', 'message': 'Message Sent'}
	def get_inbox(self,username):
		s_fr = self.get_user(username)
		incoming = s_fr['incoming']
		msgs={}
		for users in incoming:
			msgs[users]=[]
			while not incoming[users].empty():
				msgs[users].append(s_fr['incoming'][users].get_nowait())
			
		return {'status': 'OK', 'messages': msgs}
	def inbox_realm(self, session_id, username, realm_id, data):
		if (session_id not in self.sessions):
			return {'status': 'ERROR', 'message': 'Session Tidak Ditemukan'}
		if (realm_id not in self.realms):
			return {'status': 'ERROR', 'message': 'Realm Tidak Terdaftar'}

		s_fr = self.get_user(username)
		if (s_fr == False):
				return {'status': 'ERROR', 'message': 'User Tidak Terdaftar'}
			
		j = data.split()
		j[0] = "chatrealm"
		data = ' '.join(j)
		data += "\r\n"
		return self.realms[realm_id].sendstring(data)
		
	def add_realm(self, realm_id, realm_dest_address, realm_dest_port, data):
		j = data.split()
		j[0] = "recvrealm"
		data = ' '.join(j)
		data += "\r\n"
		if realm_id in self.realms:
			return {'status': 'ERROR', 'message': 'Realm sudah ada'}

		self.realms[realm_id] = RealmThreadCommunication(self, realm_dest_address, realm_dest_port)
		result = self.realms[realm_id].sendstring(data)
		return result
	def recv_realm(self, realm_id, realm_dest_address, realm_dest_port, data):
		self.realms[realm_id] = RealmThreadCommunication(self, realm_dest_address, realm_dest_port)
		return {'status':'OK'}
	def send_realm_message(self, sessionid, realm_id, username_from, username_dest, message, data):
		if (sessionid not in self.sessions):
			return {'status': 'ERROR', 'message': 'Session Tidak Ditemukan'}
		if (realm_id not in self.realms):
			return {'status': 'ERROR', 'message': 'Realm Tidak Ditemukan'}
		s_fr = self.get_user(username_from)
		s_to = self.get_user(username_dest)
		if (s_fr==False or s_to==False):
			return {'status': 'ERROR', 'message': 'User Tidak Ditemukan'}
		message = { 'msg_from': s_fr['nama'], 'msg_to': s_to['nama'], 'msg': message }
		self.realms[realm_id].put(message)
        
		j = data.split()
		j[0] = "recvrealmprivatemsg"
		j[1] = username_from
		data = ' '.join(j)
		data += "\r\n"
		self.realms[realm_id].sendstring(data)
		return {'status': 'OK', 'message': 'Message Sent to Realm'}
	def recv_realm_message(self, realm_id, username_from, username_dest, message, data):
		if (realm_id not in self.realms):
			return {'status': 'ERROR', 'message': 'Realm Tidak Ditemukan'}
		s_fr = self.get_user(username_from)
		s_to = self.get_user(username_dest)
		if (s_fr==False or s_to==False):
			return {'status': 'ERROR', 'message': 'User Tidak Ditemukan'}
		message = { 'msg_from': s_fr['nama'], 'msg_to': s_to['nama'], 'msg': message }
		self.realms[realm_id].put(message)
		return {'status': 'OK', 'message': 'Message Sent to Realm'}
	def send_group_realm_message(self, sessionid, realm_id, username_from, usernames_to, message, data):
		if (sessionid not in self.sessions):
			return {'status': 'ERROR', 'message': 'Session Tidak Ditemukan'}
		if realm_id not in self.realms:
			return {'status': 'ERROR', 'message': 'Realm Tidak Ditemukan'}
		s_fr = self.get_user(username_from)
		for username_to in usernames_to:
			s_to = self.get_user(username_to)
			message = {'msg_from': s_fr['nama'], 'msg_to': s_to['nama'], 'msg': message }
			self.realms[realm_id].put(message)
        
		j = data.split()
		j[0] = "recvrealmgroupmsg"
		j[1] = username_from
		data = ' '.join(j)
		data +="\r\n"
		self.realms[realm_id].sendstring(data)
		return {'status': 'OK', 'message': 'Message Sent to Group in Realm'}

if __name__=="__main__":
	j = Chat()
	sesi = j.proses("auth nur surabaya")
	print(sesi)
	#sesi = j.autentikasi_user('messi','surabaya')
	#print sesi
	tokenid = sesi['tokenid']
	print(j.proses("send {} nur hello gimana kabarnya rendi " . format(tokenid)))
	print(j.proses("send {} afif hello gimana kabarnya dhafin " . format(tokenid)))
	print(j.send_file(tokenid,'nur','rendi'))
	#print j.send_message(tokenid,'afif','nur','hello son')
	#print j.send_message(tokenid,'nur','afif','hello si')
	#print j.send_message(tokenid,'lineker','afif','hello si dari lineker')


	# print("isi mailbox dari afif")
	# print(j.get_inbox('afif'))
	# print("isi mailbox dari nur")
	# print(j.get_inbox('nur'))















