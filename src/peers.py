import Pyro4
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
from multiprocessing import Process
import random
import time
import json
import copy
from database import DbHandler
from LamportClock import LamportClock

from datetime import datetime


class Peer(Process):
    def __init__(self,id,role,items, items_count,host,base_path):

        # To inherit the thread class it is necessary to inherit the init and 
        # run method of the thread class
        Process.__init__(self)
        self.id = id
        self.role  = role
        self.items = items
        self.output = "tmp/output.txt"
        self.items_count = items_count
        self.item = items[random.randint(0, len(items) - 1)]
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.host  = host
        self.lock_item = Lock()
        self.lock_sellers = Lock()
        self.output_array = []
        self.max_items = 10
        self.sellers = []
        self.base_path = base_path
        self.neighbors = []
        self.received_ok_message = False
        self.received_won_message = False
        self.send_winning_message = False
        self.current_trader_id = None
        self.election_flag = False
        self.election_lock = Lock()
        self.winning_lock = Lock()
        self.price = None
        self.has_deposited = False
        self.db = DbHandler()
        self.has_deposited_lock = Lock()
        self.trading_queue = []
        self.item_lock = []
        self.trading_lock = Lock()
        self.heartbeat_counter = 0
        self.heatbeat_lock = Lock()
        self.smallest_buyer = "buyer0"
        self.clock = LamportClock()
        self.clock_lock = Lock()
        

    def __str__(self):
        return f'id : {self.id},role : {self.role},nameserver: {self.name_server},items: {self.items}, "hop_count":{self.hop_count})'

    @Pyro4.expose
    def ping(self):
        """
        Test function to check proxy
        """
        return "Pong"
    
    def get_nameserver(self):
        """
        Returns: the endpoint (reference) of the nameserver currently running 
        """
        return Pyro4.locateNS(host=self.host)
    # Get pyro4 uri for each neighbor
    def get_uri_from_id(self,peer_id):
        neighbor_uri = self.get_nameserver().lookup(peer_id)
        neighbor_proxy = Pyro4.Proxy(neighbor_uri)
        return neighbor_proxy

    def check_higher_id(self,id_2):
        id_1_index = int(self.id[-1])
        id_2_index = int(id_2[-1])

        return True if id_2_index>id_1_index else False

    
                    
            
    def forward_win_message(self):
        print(f"{self.id} has won the election and is now the new trader of the bazaar!!!")
        self.received_won_message = True
        self.election_flag = False
        self.current_trader_id = self.id
        self.role = "trader"
        pending_transactions = self.db.fetch_pending_transactions()
        if pending_transactions !=None:
            self.trading_queue = pending_transactions["data"][:]
        for neighbor in self.neighbors:
            self.executor.submit(self.get_uri_from_id(neighbor).send_election_message ,"won",self.id)

        # Increase the number of workers for trader as Trader has a lot to do in 
        # current architecture
        self.executor = ThreadPoolExecutor(max_workers=50)

        print("Trader is ready to trade")
        self.executor.submit(self.begin_trading)

    # Trader will call sick after serving 40 requests
    @Pyro4.expose
    def send_heartbeat(self):
        if self.heartbeat_counter<40:
            return "alive"
        else:
            self.election_lock.acquire()
            self.election_flag = True
            self.election_lock.release()
            return "dead"

   

    # This method elects the leader using Bully algorithm
    @Pyro4.expose
    def elect_leader(self):
        try:
            print(f"{self.id} has started the election")
            # time.sleep(1)
            higher_peers = []
            
            for neigh_id in self.neighbors:
                if neigh_id == self.current_trader_id:
                    continue
                if self.check_higher_id(neigh_id):
                    higher_peers.append(self.get_uri_from_id(neigh_id))
            if len(higher_peers)>0:
                # Acquire the election lock and set election running status to true
                self.election_lock.acquire()
                self.election_flag = True
                self.election_lock.release()
            

                for higher_peer in higher_peers:
                    self.executor.submit(higher_peer.send_election_message ,"elect_leader",self.id)
                time.sleep(5)
                if self.received_ok_message == False and  self.received_won_message == False:
                    self.winning_lock.acquire()
                    self.send_winning_message = True
                    self.forward_win_message()
                    self.winning_lock.release()
    
            else:
                self.winning_lock.acquire()
                self.send_winning_message = True
                self.forward_win_message()
                self.winning_lock.release()
        except Exception as e:
            print(f"Something went wrong while {self.id} tried to start election with error {e}")
        


    '''
    This function handles three types of messages
    elect_leader : If this message is received by the peer then it will first respond to the sender 
    by an ok message and forward the election message to its neighbors
    ok: Drop out of the election
    I won : Recived the message from the new leader set winning trader as the new co-ordinator of the 
    bazaar
    '''
    @Pyro4.expose
    def send_election_message(self,message,sender):
        try:
            if message == "elect_leader":
                self.executor.submit(self.get_uri_from_id(sender).send_election_message,"OK",sender)
                # If the peer haven't taken part in election till now only then it will take part in an election
                if not self.received_ok_message and not self.received_won_message:
                    higher_peers = []
                    for neigh_id in self.neighbors:
                        if neigh_id == self.current_trader_id:
                            continue
                        if self.check_higher_id(neigh_id):
                            higher_peers.append(self.get_uri_from_id(neigh_id))
                    if len(higher_peers)>0:
                        # Acquire the election lock and set election running status to true
                        self.election_lock.acquire()
                        self.election_flag = True
                        self.election_lock.release()
                        
                        for higher_peer in higher_peers:
                            self.executor.submit(higher_peer.send_election_message ,"elect_leader",self.id)
                        
                        time.sleep(5)

                        # check for the winning case
                        # If the peer haven't received an Ok or won message that message it is the winner of the election
                        # and will be crowned as the leader
                        if self.received_ok_message == False and self.received_won_message == False:
                            self.winning_lock.acquire()
                            self.send_winning_message = True
                            self.forward_win_message()
                            self.winning_lock.release()
                    else:
                        self.winning_lock.acquire()
                        self.send_winning_message = True
                        self.forward_win_message()
                        self.winning_lock.release()
            elif message == "OK":
                self.received_ok_message = True
            elif message == "won":
                print(f"{self.id} received message won from {sender} and recognizes {sender} as the new coordinator of the bazaar")
                self.winning_lock.acquire()
                self.received_won_message = True
                self.current_trader_id = sender
                self.election_flag = False
                self.winning_lock.release()
                # Sleep for sometime before trading begins
                time.sleep(10)
                print(f"Election has completed succesfully and {self.id} is ready to trade")
                self.executor.submit(self.begin_trading)
        except Exception as e:
            print(f"Something went wrong with error {e}")
    # broadcast_lamport_clock : This method broadcasts a peer's clock to all the peers.
    def broadcast_lamport_clock(self):
        for neighbor in self.neighbors:
            neighbor_proxy = self.get_uri_from_id(neighbor)
            self.executor.submit(neighbor_proxy.adjust_buyer_clock, self.clock.value)
            
    
    # adjust_buyer_clock: Upon receiving this message, a peer adjusts its clock, 
    # only buyer and seller adjust there clock, but not the trader.
    # Trader adjust their clock when lookup 
    @Pyro4.expose
    def adjust_buyer_clock(self, sender_clock):
        # print("Lamport clock adjusted")
        try:
            if self.role == "buyer" : 
                self.clock_lock.acquire()
                self.clock.adjust(sender_clock) 
                self.clock_lock.release()
        except Exception as e:
            print(f"Something went wrong while trying to adjust buyer's clock with error {e}")

    @Pyro4.expose
    def begin_trading(self):
        try:
        
            # Set all election related flags to false
            self.winning_lock.acquire()
            self.received_won_message = False
            self.received_ok_message = False
            self.send_winning_message = False
            self.heartbeat_counter = 0
            self.election_flag = False
            self.winning_lock.release()

            # When trading begins seller will deposit all their items to trader if they haven't done so
            
            if self.role == "seller" and not self.has_deposited:
                trader = self.current_trader_id
                trader_proxy = self.get_uri_from_id(trader)
                item = self.item
                count  = self.items_count
                price = self.price

                seller_data = {"seller_id":self.id, "count":count, "price":price, "item":item}
                registration_result = self.executor.submit(trader_proxy.register_product, seller_data, self.id)
                res = registration_result.result()
                if res:
                    print(f"{self.id} registered their product {self.item} with trader")
                    self.has_deposited_lock.acquire()
                    self.has_deposited = True
                    self.has_deposited_lock.release()
                else:
                    print(f"Something went wrong while registering the product with trader. Retrying!!!")
                    self.begin_trading()
            # Trader loop will take care of purchase request from buyer
            elif self.role == "trader":
                
                self.executor.submit(self.trader_loop)
            # If role is of buyer then start buyer loop and keep on buying product
            elif self.role == "buyer":
                # Wait for sellers to register their product with trader
                time.sleep(5)
                self.executor.submit(self.buyer_loop)
        except Exception as e:
            print(f"Something went wrong while starting to trade for {self.id} with error {e}")
    
    @Pyro4.expose              
    def add_to_trading_queue(self, buyer_id, item, current_clock):
        self.trading_queue.append((item, buyer_id, current_clock))
     


    # This method starts buyer loop for buyer using which they can buy any product
    def buyer_loop(self):
        
        while True:
            try:
                trader = self.get_uri_from_id(self.current_trader_id)
                executor = self.executor.submit(trader.send_heartbeat)
                res = executor.result()
                if res == "dead":
                    if self.id !=self.smallest_buyer:
                        self.election_lock.acquire()
                        self.election_flag = True
                        self.election_lock.release()
                        break
                    else:
                        self.election_lock.acquire()
                        self.election_flag = True
                        self.election_lock.release()
                        print("Current leader is dead!!! Start re-election")
                        self.elect_leader()
                        break
                self.clock_lock.acquire()
                
                current_clock = self.clock.value
                # self.executor.submit(self.broadcast_lamport_clock)
                print("REached here!!!")
              
                self.executor.submit(trader.add_to_trading_queue, self.id, self.item, current_clock)
                time.sleep(10)
                
                self.item = self.items[random.randint(0, len(self.items) - 1)]
            except Exception as e:
                print(f"Something went wrong in the buyer loop with error {e} ")
            
        
    @Pyro4.expose
    def send_purchase_message(self, seller_id, item):
        print(f"{self.id} purchased {item} from {seller_id}")
        self.item_lock.aquire()
        self.item = self.items[random.randint(0, len(self.items) - 1)]
        self.item_lock.release()
        print(f"{self.id} has now picked item {self.item} to purchase")
        
        

    # Trader will sell the product to buyer using this function they will also send a message to the seller 
    # whose product was sold with remaining number of products and commission amount
    def trader_loop(self):
        while True:
            if self.election_flag:
                
                break
            try:
                if len(self.trading_queue)>0:
                    # Write all the pending transactions to the disk

                    pending_transactions = {"data":self.trading_queue}
                    try:
                        thread = Thread(target=self.db.save_transactions, args=(pending_transactions,))
                        thread.start()
                        # thread.join()

                        # self.executor.submit(self.db.save_transactions,pending_transactions)
                    except Exception as e:
                        print("Something went wrong while trying to write pending transactions to disk with error {e}")
         
                    # Find the item in the queue with least clock value and serve it 
                    mini = float("inf")
                    min_index = None
                    item, buyer_id , clock= self.trading_queue.pop(0)
                    
                    # try:
                    #     for i, item_tuple in enumerate(self.trading_queue):
                    #         print(item_tuple)
                    #         item, buyer_id, clock = item_tuple
                    #         if clock<mini:
                    #             min_index = i
                    #             mini = clock
                    #     item, buyer_id , clock= self.trading_queue.pop(min_index)
                    # except Exception as e:
                    #     print(f"Something went wrong while trying to fetch data from trading queue {e}")
                    # Trader will adjust it's clock when ever a request comes to it
                 
                    # self.clock.adjust(clock) 
                    
                    print(f"Trader got a purchase request for item {item} from {buyer_id} with clock {clock}")
                    data = self.db.find_seller_by_item(item)
                   
                    
                    if data == None:
                        print(f"{item} is not available for sell in the bazaar right now")
                        continue
                    seller_id = data["seller_id"]
                    price = data["price"]
                    count = data["count"]
                    

                    seller_proxy = self.get_uri_from_id(seller_id)
                    buyer_proxy = self.get_uri_from_id(buyer_id)

                    self.db.insert_into_database({"seller_id":seller_id, "count":count-1, "price":price, "item":item})
                    # Upon each succesful trade increase the hearbeat counter by 1
                    self.heatbeat_lock.acquire()
                    self.heartbeat_counter+=1
                    self.heatbeat_lock.release()
                    self.executor.submit(seller_proxy.send_sale_message, item, round(0.8*price,2), count-1, buyer_id, False)
                    self.executor.submit(buyer_proxy.send_purchase_message, seller_id, item)
  
              
            except Exception as e:
                print(f"Registering product for {seller_id} failed with error{e}")
                return False

    @Pyro4.expose
    def send_sale_message(self, item, commission, count, buyer_id, zero_flag):

        if count>=0:
            print(f"{self.id} has sold {item} to {buyer_id} and earned {commission} $")
            print(f"{self.id} has {count} {item} left")
        if count <=0:
            print(f"{self.id} is out of stock for item {item}")
            while True:
                picked_item = self.items[random.randint(0, len(self.item) - 1)]
                if self.item!=picked_item:
                    self.item = picked_item
                    break
            print(f"{self.id} has picked item {self.item} to sell")
            self.has_deposited_lock.acquire()
            self.has_deposited = False
            self.has_deposited_lock.release()
            self.begin_trading()


    # This method registers products of each seller
    @Pyro4.expose
    def register_product(self, seller_info, seller_id):
        try:
            self.db.insert_into_database(seller_info)
            seller_data = self.db.fetch_one_from_database(seller_id)
            if seller_data == None or seller_data["count"] == 0:
                return False
            return True
        except Exception as e:
            print(f"Registering product for {seller_id} failed with error{e}")
            return False
        

                       
    def get_timestamp(self):
        """
        Returns: Current Timestamp
        """
        return datetime.now()

    # Inheriting the run method of thread class
    def run(self):
        """
        Starting point of Peer creation
        Creates objects and registers them as pyro objects. Registers pyro objects to namespace.
        Starts the pyro request loop. Initiates lookups for buyer. Calls the buy method if a seller
        is matched. 
        """
        try:
            # Registering the peer as Pyro5 object
            with Pyro4.Daemon(host = self.host) as daemon:
                try:
                    # Registers peers as pyro object and start daemon thread
                    peer_uri = daemon.register(self)
                    # Registers the pyro object on the nameserver and creates a mapping
                    self.get_nameserver().register(self.id,peer_uri)

                except Exception as e:
                    print(f"Registring to nameserver failed with error {e}")
                if self.role == "buyer":
                    print(f"{self.get_timestamp()} - {self.id} joined the bazar as buyer looking for {self.item}")
                else:
                    print(f"{self.get_timestamp()} - {self.id} joined the bazar as seller selling {self.item}")
                
                # Start the Pyro requestLoop
                self.executor.submit(daemon.requestLoop)
                # Sleep for sometime so that all peers join Bazaar
                time.sleep(4)
                if int(self.id[-1])==2:
                    self.elect_leader()
                while True:
                    time.sleep(1)
        
        except Exception as e:
            print(f"Something went wrong in run method with exception {e}")
    
    