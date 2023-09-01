#Message Broker

##How does it work?
These are two Ruby programs, a producer which make possible for the user create a message and send it to a consumer through a message Broker (redis in this case).

Each program has a sqlite database where the message data and information about their delivery are stored.

The producer check from the db if there's some message to be sent and do the sending, if the broker answer with success, then the producer marks as sent the message.

The entire system should be fault-tolerant, preventing the loss of the messages in case of each of the three component (producer, broker and consumer) goes down.

##Usage
###Preliminary
1. Clone the repo
2. Make sure you have **redis** installed on your system. Using linux I used the following (snap) command:
    ```bash
    sudo snap install redis
    ```
3. Make sure you have Ruby installed on your system (along with **gem** and **bundler**)
4. Run the following command in the project root for correctly install the dependencies:
   ```bash
   bundle install
   ```
###Run

Run the producer launching this command in the project root:
```bash
ruby producer_bak.rb
```

Run the consumer launching this command in the project root:
```bash
ruby consumer.rb
```