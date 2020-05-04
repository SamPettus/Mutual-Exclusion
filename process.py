import socket
import sys
import threading
from queue import Queue
import operator
import time

#Header size
HEADERSIZE = 10

#LinkedList Classes
class Node:
    def __init__(self, data):
        self.data = data
        self.next = None
class LinkedList:
    def __init__(self):
        self.head = None
    def printList(self):
        temp = self.head
        print("[", end = "")
        while(temp):
            print(temp.data, end = " ")
            temp = temp.next
        print("]")

def initializeSocket(portNum):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #TCP socket
    s.connect((socket.gethostname(), portNum))
    return s

def recieve(s, q):
    while True:
        full_msg = ''
        new_msg = True
        while True: #Buffers data
            msg = s.recv(16) #Determines chunk size
            if new_msg:
                msglen = int(msg[:HEADERSIZE])
                new_msg = False

            full_msg += msg.decode("utf-8")

            if len(full_msg) - HEADERSIZE == msglen:
                print("full msg recieved!")
                print(full_msg[HEADERSIZE:])
                full_msg = full_msg[HEADERSIZE:]
                q.put(full_msg)
                new_msg = True
                full_msg = ''


        print(full_msg)#Byte SOCK_STREAM

def processingThread(q, lamportClock, s, processID, lamportQueue, ll, balance):
    #Tracks the number of replies
    replyCounter = 0
    while True:
        if not q.empty():
            #Message format is [type, portNum, rec, amt, lamportClock]
            msg = q.get()
            events = msg.split(',')
            #Request Message
            if events[0] == "request":
                while(lamportClock.empty()):
                    pass
                temp = lamportClock.get()

                #Update Lamport Clock
                temp +=1
                lamportClock.put(temp)

                #Update Task Queue ------
                lamportQueue.put((int(temp), int(processID), int(events[2]), int(events[3])))
                #Send message
                sendMsg = events[0] + ',' + events[1] + ',' + events[2] + ',' + events[3] + ',' + str(temp)
                sendMsg = f'{len(sendMsg):<{HEADERSIZE}}' + sendMsg
                s.send(bytes(sendMsg, "utf-8"))
            if events[0] == "recieve":
                #Update Lamport Clock
                while(lamportClock.empty()):
                    pass
                temp = lamportClock.get()
                temp = max(temp, int(events[4])) + 1
                lamportClock.put(temp)

                #Check our Queue
                if lamportQueue.empty():
                    while(lamportClock.empty()):
                        pass
                    temp = lamportClock.get()
                    temp = max(temp, int(events[4])) + 1
                    lamportClock.put(temp)
                    sendMsg = 'reply,' + events[1] + ',' + events[2] + ',' + events[3] + ',' + str(temp)
                    sendMsg = f'{len(sendMsg):<{HEADERSIZE}}' + sendMsg
                    s.send(bytes(sendMsg, "utf-8"))
                else:
                    lamportQueue.put((int(events[4]), int(events[1]), int(events[2]), int(events[3])))
                    tempList = []
                    while(not(lamportQueue.empty())):
                        x = lamportQueue.get()
                        tempList.append(x)
                    tempList = sorted(tempList)
                    first = tempList[0]
                    #If first element in Queue is the request, send a respone. Otherwise wait.
                    if int(first[1]) == int(events[1]):
                        while(lamportClock.empty()):
                            pass
                        temp = lamportClock.get()
                        temp = max(temp, int(events[4])) + 1
                        lamportClock.put(temp)
                        sendMsg = 'reply,' + events[1] + ',' + events[2] + ',' + events[3] + ',' + str(temp)
                        sendMsg = f'{len(sendMsg):<{HEADERSIZE}}' + sendMsg
                        s.send(bytes(sendMsg, "utf-8"))
                    #Otherwise keep waiting
                    else:
                        for tasks in tempList:
                            lamportQueue.put(tasks)

            if events[0] == "reply":
                #Update lamportClock
                while(lamportClock.empty()):
                    pass
                temp = lamportClock.get()
                temp = max(temp, int(events[4])) + 1
                lamportClock.put(temp)
                replyCounter +=1
                if(replyCounter==2):
                    #Update your balance
                    while(balance.empty()):
                        pass
                    tempBalance = balance.get()
                    tempBalance = tempBalance - int(events[3])
                    balance.put(tempBalance)
                    #Remove your request from the Queue
                    tempList = []
                    while(not(lamportQueue.empty())):
                        x = lamportQueue.get()
                        tempList.append(x)
                    filterList = [i for i in tempList if int(i[1]) != int(events[1])]
                    for task in filterList:
                        lamportQueue.put(task)
                    #Reset reply counter
                    replyCounter = 0
                    print("Recieved all replies. Entering Critical Section")
                    #Update LinkedList
                    if(ll.head == None):
                        ll.head = Node("(" + events[1] + ',' + events[2] + ',$' + events[3] + ')')
                    else:
                        temp = ll.head
                        while(temp.next != None):
                            temp = temp.next
                        temp.next = Node("(" + events[1] + ',' + events[2] + ',$' + events[3] + ')')

                    #Update Lamport Clock
                    while(lamportClock.empty()):
                        pass
                    temp = lamportClock.get()
                    temp +=1
                    lamportClock.put(temp)

                    #Send broadcast Message
                    sendMsg = 'broadcast,' + events[1] + ',' + events[2] + ',' + events[3] + ',' + str(temp)
                    sendMsg = f'{len(sendMsg):<{HEADERSIZE}}' + sendMsg
                    s.send(bytes(sendMsg, "utf-8"))
                    time.sleep(.2)
                    if not(lamportQueue.empty()):
                        tempList = []
                        #Reply to next most important message
                        while(not(lamportQueue.empty())):
                            x = lamportQueue.get()
                            tempList.append(x)
                        tempList = sorted(tempList)
                        #Update Clock
                        while(lamportClock.empty()):
                            pass
                        temp = lamportClock.get()
                        temp = max(temp, int(tempList[0][0])) + 1
                        lamportClock.put(temp)
                        #Send out Reply
                        sendMsg = 'reply,' + str(tempList[0][1]) + ',' + str(tempList[0][2]) + ',' + str(tempList[0][3]) + ',' + str(temp)
                        sendMsg = f'{len(sendMsg):<{HEADERSIZE}}' + sendMsg
                        s.send(bytes(sendMsg, "utf-8"))

            if events[0] == "broadcast":
                #Update Lamport Clock
                while(lamportClock.empty()):
                    pass
                tem = lamportClock.get()
                tem = max(tem, int(events[4])) + 1
                lamportClock.put(tem)
                #Remove broadcasted task from the queue
                tempList = []
                while(not(lamportQueue.empty())):
                    x = lamportQueue.get()
                    tempList.append(x)
                for task in tempList:
                    if int(task[1]) != int(events[1]):
                        lamportQueue.put(task)
                #If broadcasted transaction involves reciever
                if str(events[2]) == str(processID):
                    while(balance.empty()):
                        pass
                    tempBalance = balance.get()
                    tempBalance += int(events[3])
                    balance.put(tempBalance)
                #Update local linked list
                if(ll.head == None):
                    ll.head = Node("(" + events[1] + ',' + events[2] + ',$' + events[3] + ')')
                else:
                    temp = ll.head
                    while(temp.next != None):
                        temp = temp.next
                    temp.next = Node("(" + events[1] + ',' + events[2] + ',$' + events[3] + ')')


def main():
    #Initial Balance
    balance = Queue()
    balance.put(10)
    #Initial LamportClock State
    lamportClock = Queue()
    lamportClock.put(0)
    #Linked List
    ll = LinkedList()
    #"Queue"
    lamportQueue = Queue()


    q = Queue()
    #Gets port number from command line
    portNum = int(sys.argv[1])
    s = initializeSocket(portNum)
    #Creates communication thread
    communication = threading.Thread(target=recieve, args=(s,q ))
    communication.start()
    #Connection Message
    processing = threading.Thread(target=processingThread, args=(q, lamportClock, s, portNum, lamportQueue, ll, balance))
    processing.start()

    #Loop Logic
    while True:
        x = int(input('1 for Transfer Event, 2 Print Balance, and 3 for Print Blockchain: '))
        if x == 1:
            #Update clock
            while(lamportClock.empty()):
                pass
            temp = lamportClock.get()
            temp +=1
            lamportClock.put(temp)

            #Get input
            amt, rec = input(str(portNum) + ' transfer event! Input amount and reciever: ').split()

            #Check balance
            while(balance.empty()):
                pass
            tempBalance = balance.get()
            #Check if valid input
            if int(amt) > tempBalance:
                balance.put(tempBalance)
                print("Failure")
                pass
            else:
                balance.put(tempBalance)
                #Format Message
                msg = 'request,' + str(portNum) + ',' + rec + ',' + amt
                q.put(msg)
        if x == 2:
            while(balance.empty()):
                pass
            tempBalance = balance.get()
            print(str(portNum) + " Balance: $" + str(tempBalance))
            balance.put(tempBalance)
        if x == 3:
            ll.printList()



main()
