#pragma once

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
#include <string>

class RabbitMQ {
private:
    amqp_connection_state_t connection_{nullptr};
    amqp_socket_t* socket_{nullptr};
    amqp_bytes_t consumer_tag_{amqp_empty_bytes};
    bool is_connected_{false};

public:
    RabbitMQ() = default;
    
    ~RabbitMQ()
    {
        disconnect();
    }

    RabbitMQ(const RabbitMQ&) = delete;
    RabbitMQ& operator=(const RabbitMQ&) = delete;

    bool connect(const std::string& host,
                 const int port,
                 const std::string& user,
                 const std::string& password)
    {
        if ( is_connected_ ) { return true; }

        connection_ = amqp_new_connection();
        socket_ = amqp_tcp_socket_new(connection_);
        if ( not socket_ ) {
            amqp_destroy_connection(connection_);
            connection_ = nullptr;
            return false;
        }

        int status = amqp_socket_open(socket_, host.c_str(), port);
        if ( status != AMQP_STATUS_OK ) {
            amqp_destroy_connection(connection_);
            connection_ = nullptr;
            socket_ = nullptr;
            return false;
        }

        amqp_rpc_reply_t reply = amqp_login(connection_, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                                            user.c_str(), password.c_str());
        if ( reply.reply_type != AMQP_RESPONSE_NORMAL ) {
            amqp_destroy_connection(connection_);
            connection_ = nullptr;
            socket_ = nullptr;
            return false;
        }

        amqp_channel_open(connection_, 1);
        reply = amqp_get_rpc_reply(connection_);
        if ( reply.reply_type != AMQP_RESPONSE_NORMAL ) {
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
            connection_ = nullptr;
            socket_ = nullptr;
            return false;
        }

        is_connected_ = true;
        return true;
    }

    bool disconnect()
    {
        if ( not is_connected_ ) { return true; }

        if ( connection_ ) {
            if ( not consumer_tag_.bytes ) {
                amqp_basic_cancel(connection_, 1, consumer_tag_);
                consumer_tag_ = amqp_empty_bytes;
            }
            
            amqp_channel_close(connection_, 1, AMQP_REPLY_SUCCESS);
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
            connection_ = nullptr;
            socket_ = nullptr;
        }

        is_connected_ = false;
        return true;
    }

    bool declareQueue(const std::string& queueName)
    {
        if ( not is_connected_ ) { return false; }

        amqp_queue_declare(connection_, 1, amqp_cstring_bytes(queueName.c_str()),
                          0, 1, 0, 0, amqp_empty_table);

        amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection_);
        return reply.reply_type == AMQP_RESPONSE_NORMAL;
    }

    bool sendMessage(const std::string& message, const std::string& queueName)
    {
        if ( not is_connected_ ) { return false; }

        amqp_bytes_t message_bytes;
        message_bytes.len = message.size();
        message_bytes.bytes = const_cast<void*>(static_cast<const void*>(message.c_str()));

        int status = amqp_basic_publish(connection_, 1, amqp_empty_bytes, 
                                       amqp_cstring_bytes(queueName.c_str()),
                                       0, 0, nullptr, message_bytes);
        return status == AMQP_STATUS_OK;
    }

    bool startConsuming(const std::string& queueName)
    {
        if ( not is_connected_ ) { return false; }

        amqp_basic_consume_ok_t* consume_ok = amqp_basic_consume(connection_, 1, 
                                                                 amqp_cstring_bytes(queueName.c_str()),
                                                                 amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection_);
        if ( reply.reply_type != AMQP_RESPONSE_NORMAL ) { return false; }

        consumer_tag_ = consume_ok->consumer_tag;
        return true;
    }

    bool receiveMessage(std::string& message, int timeout_sec = 1)
    {
        if ( not is_connected_ ) { return false; }

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(connection_);

        struct timeval timeout;
        timeout.tv_sec = timeout_sec;
        timeout.tv_usec = 0;

        amqp_rpc_reply_t reply = amqp_consume_message(connection_, &envelope, &timeout, 0);
        
        if ( reply.reply_type == AMQP_RESPONSE_NORMAL ) {
            message = std::string(static_cast<const char*>(envelope.message.body.bytes),
                                 envelope.message.body.len);
            
            uint64_t delivery_tag = envelope.delivery_tag;
            amqp_destroy_envelope(&envelope);
            
            amqp_basic_ack(connection_, 1, delivery_tag, 0);
            return true;
        } else if ( reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION and
                    reply.library_error == AMQP_STATUS_TIMEOUT ) { return false; }
        
        return false;
    }

    bool isConnected() const
    {
        return is_connected_;
    }
};