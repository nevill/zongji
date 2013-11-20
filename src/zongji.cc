#include "connection.h"

#include <mysql_com.h>
#include <sql_common.h>

#include <string>

using namespace v8;

namespace zongji {

  namespace internal {

    uchar* net_store_data(uchar* destination, const uchar* source, size_t length)
    {
      destination = net_store_length(destination, length);
      memcpy(destination, source, length);
      return destination + length;
    }

    bool Connection::connect(const char* user, const char* password,
                  const char* host, uint port, size_t offset) {

      m_mysql= mysql_init(NULL);

      uchar buf[1024];
      uchar* pos = buf;

      /* So that mysql_real_connect use TCP_IP_PROTOCOL. */
      // mysql_unix_port =0;
      int server_id = 1;
      // MYSQL_RES* res = 0;
      // MYSQL_ROW row;
      // const char* checksum;
      // uchar version_split[3];

      /*
        Attempts to establish a connection to a MySQL database engine
        running on host

        Returns a MYSQL* connection handle if the connection was successful,
        NULL if the connection was unsuccessful.
        For a successful connection, the return value is the same as
        the value of the first parameter.
      */
      if (!mysql_real_connect(m_mysql, host, user, password, "", port, 0, 0)) {
        m_error = mysql_error(m_mysql);
        return false;
      }

      int4store(pos, server_id);
      pos += 4;
      pos = net_store_data(pos, (const uchar*) host, strlen(host));
      pos = net_store_data(pos, (const uchar*) user, strlen(user));
      pos = net_store_data(pos, (const uchar*) password, strlen(password));
      int2store(pos, (uint16) port);
      pos += 2;

      /*
        Fake rpl_recovery_rank, which was removed in BUG#13963,
        so that this server can register itself on old servers,
        see BUG#49259.
      */
      int4store(pos, /* rpl_recovery_rank */ 0);
      pos += 4;

      /* The master will fill in master_id */
      int4store(pos, 0);
      pos += 4;

      /*
        register this connection as a slave
        it requires priviledge 'replication slave' to execute

        GRANT REPLICATION SLAVE ON *.* TO 'user'@'%.domain.com';
      */
      if (simple_command(m_mysql, COM_REGISTER_SLAVE, buf, (size_t) (pos - buf), 0)) {
        m_error = mysql_error(m_mysql);
        return false;
      }

      // const char* binlog_file = "";
      // start_binlog_dump(binlog_file, offset);
      return true;
    }
  }

  Handle<Value> Connection::NewInstance(const Arguments& args) {
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
    tpl->SetClassName(String::NewSymbol("Connection"));

    NODE_SET_PROTOTYPE_METHOD(tpl, "waitForNextEvent", Connection::WaitForNextEvent);
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", Connection::Connect);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Persistent<Function> constructor = Persistent<Function>::New(tpl->GetFunction());
    return scope.Close(constructor->NewInstance(0, NULL));
  }

  Handle<Value> Connection::WaitForNextEvent(const Arguments& args) {
    HandleScope scope;
    return scope.Close(Undefined());
  }

  Handle<Value> Connection::Connect(const Arguments& args) {
    HandleScope scope;

    if (args.Length() < 4) {
      ThrowException(Exception::TypeError(String::New("Wrong number of arguments")));
    }
    else if (!args[0]->IsString()) {
      ThrowException(Exception::TypeError(String::New("Wrong argument user")));
    }
    else if (!args[1]->IsString()) {
      ThrowException(Exception::TypeError(String::New("Wrong argument password")));
    }
    else if (!args[2]->IsString()) {
      ThrowException(Exception::TypeError(String::New("Wrong argument host")));
    }
    else if (!args[3]->IsNumber()) {
      ThrowException(Exception::TypeError(String::New("Wrong port number")));
    }
    else {
      std::string user = std::string(*String::Utf8Value(args[0]->ToString()));
      std::string password = std::string(*String::Utf8Value(args[1]->ToString()));
      std::string host = std::string(*String::Utf8Value(args[2]->ToString()));
      uint port = args[3]->Uint32Value();

      Connection* conn = ObjectWrap::Unwrap<Connection>(args.This());
      bool result = conn->m_connection->connect(user.c_str(), password.c_str(), host.c_str(), port);
      if (!result) {
        ThrowException(Exception::TypeError(
          String::New(conn->m_connection->m_error)));
      }
    }

    return scope.Close(Undefined());
  }

  Handle<Value> Connection::New(const Arguments& args) {
    HandleScope scope;

    Connection* obj = new Connection();
    obj->Wrap(args.This());
    return args.This();
  }
}

Handle<Value> InitConnection(const Arguments& args) {
  HandleScope scope;
  return scope.Close(zongji::Connection::NewInstance(args));
}

void init(Handle<Object> exports) {
  exports->Set(String::NewSymbol("init"),
               FunctionTemplate::New(InitConnection)->GetFunction());
}

NODE_MODULE(zongji, init)
