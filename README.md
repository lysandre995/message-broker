# Message Broker

## How does it work?

These are two Ruby programs, a **producer** which allows the user to create a message and send it to a **consumer** that receives them through a message Broker (**Redis** in this case).

Each program has an **SQLite 3** database where the message data and information about their delivery are stored in.

The **producer** checks the database for pending messages to be sent and performs the sending. If the **broker** responds with success, then the **producer** marks the messages as sent.

The whole system should be fault-tolerant, preventing message loss in case any of the three components (**producer**, **broker** and **consumer**) goes down.

Due to the blocking nature of the user input insertion command, any pending messages in the **producer** will be sent subsequently upon the user's submission of another message to be sent.

On the other hand, if the pending messages are stored in the **broker**'s queue, they will be instantly available when the **consumer** or the **broker** resumes working properly.

## Usage

### Preliminary

1. Clone the repo
2. Ensure you have **Redis** installed on your system. On Linux you can use the following (snap) command:
    ```bash
    sudo snap install redis
    ```
3. Make sure you have Ruby installed on your system (along with **gem** and **bundler**)
4. Run the following command from the project root to correctly install the dependencies:
   ```bash
   bundle install
   ```
### Run

Run the **producer** and the **consumer** in two different terminals.

Launch the **producer** by typing this command from the project root:
```bash
ruby producer.rb
```

Launch the **consumer** by typing this command from the project root:
```bash
ruby consumer.rb
```

## Stop the redis service for testing purposes

To disable the redis service on Linux using snap (different command should be used based on your installation, refer to your Redis installation documentation):
```bash
sudo snap stop redis
```

To restart the redis service on linux using snap:
```bash
sudo snap start redis
```
