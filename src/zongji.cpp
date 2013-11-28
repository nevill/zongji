#include "connection.h"

#include <mysql_com.h>
#include <sql_common.h>

#include <string>

#include <node.h>
#include <node_version.h>
#include <node_buffer.h>

using namespace v8;

namespace zongji {

  namespace internal {

    uchar* netStoreData(uchar* destination, const uchar* source, size_t length)
    {
      destination = net_store_length(destination, length);
      memcpy(destination, source, length);
      return destination + length;
    }

    bool Connection::connect(const char* user, const char* password,
                  const char* host, uint port) {

      m_mysql= mysql_init(NULL);

      uchar buf[1024];
      uchar* pos = buf;
      int server_id = 1;

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
      pos = netStoreData(pos, (const uchar*) host, strlen(host));
      pos = netStoreData(pos, (const uchar*) user, strlen(user));
      pos = netStoreData(pos, (const uchar*) password, strlen(password));
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

      return true;
    }

    bool Connection::beginBinlogDump(size_t offset) {
      const char* binlog_name = "";

      // see http://dev.mysql.com/doc/internals/en/com-binlog-dump.html
      uchar buf[1024];

      ushort binlog_flags = 0;
      int server_id = 1;
      size_t binlog_name_length;

      m_mysql->status = MYSQL_STATUS_READY;

      int4store(buf, long(offset));
      int2store(buf + 4, binlog_flags);
      int4store(buf + 6, server_id);

      binlog_name_length = strlen(binlog_name);

      memcpy(buf + 10, binlog_name, binlog_name_length);

      if (simple_command(m_mysql, COM_BINLOG_DUMP, buf, binlog_name_length + 10, 1)) {
        m_error = mysql_error(m_mysql);
        return false;
      }
      return true;
    }

    int Connection::nextEvent() {
      return cli_safe_read(m_mysql);
    }
  }

  Handle<Value> Connection::NewInstance(const Arguments& args) {
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
    tpl->SetClassName(String::NewSymbol("Connection"));

    NODE_SET_PROTOTYPE_METHOD(tpl, "waitForNextEvent", Connection::WaitForNextEvent);
    NODE_SET_PROTOTYPE_METHOD(tpl, "connect", Connection::Connect);
    NODE_SET_PROTOTYPE_METHOD(tpl, "beginBinlogDump", Connection::BeginBinlogDump);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Persistent<Function> constructor = Persistent<Function>::New(tpl->GetFunction());
    return scope.Close(constructor->NewInstance(0, NULL));
  }

  Handle<Value> Connection::WaitForNextEvent(const Arguments& args) {
    HandleScope scope;

    Local<Function> cb = Local<Function>::Cast(args[0]);
    const unsigned argc = 2;
    Local<Value> argv[argc];

    Connection* conn = ObjectWrap::Unwrap<Connection>(args.This());
    int length = conn->m_connection->nextEvent();
    const char* buff = (char*)conn->m_connection->m_mysql->net.buff;

    if (length > 0) {
      argv[0] = Local<Value>::New(Null());
      argv[1] = Local<Value>::New(
        node::Buffer::New(buff, length)->handle_ );
    }
    else {
      argv[0] = Exception::Error(String::New("Unknown mysql binlog error"));
      argv[1] = Local<Value>::New(Null());
    }

    cb->Call(Context::GetCurrent()->Global(), argc, argv);

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
      if (result) {
        return scope.Close(True());
      }
      else {
        ThrowException(Exception::TypeError(
          String::New(conn->m_connection->m_error)));
      }
    }

    return scope.Close(False());
  }

  Handle<Value> Connection::BeginBinlogDump(const Arguments& args) {
    HandleScope scope;

    Connection* conn = ObjectWrap::Unwrap<Connection>(args.This());
    bool result = conn->m_connection->beginBinlogDump();
    if (result) {
      return scope.Close(True());
    }
    else {
      ThrowException(Exception::TypeError(
          String::New(conn->m_connection->m_error)));
    }

    return scope.Close(False());
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
