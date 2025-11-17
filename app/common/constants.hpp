#pragma once

#include <string>

inline const std::string DB_HOST = "localhost";
inline const std::string DB_PORT = "5432";
inline const std::string DB_NAME = "textdb";
inline const std::string DB_USER = "user";
inline const std::string DB_PASSWORD = "password";
inline const std::string DB_CONN_STRING = "host=" + DB_HOST +
                                          " port=" + DB_PORT +
                                          " dbname=" + DB_NAME +
                                          " user=" + DB_USER +
                                          " password=" + DB_PASSWORD;

inline const std::string RABBITMQ_HOST = "localhost";
inline const int RABBITMQ_PORT = 5672;
inline const std::string RABBITMQ_USER = "guest";
inline const std::string RABBITMQ_PASSWORD = "guest";
inline const std::string QUEUE_NAME = "text-processing-tasks";

inline const int BATCH_SIZE = 10;