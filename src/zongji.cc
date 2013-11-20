#include "connection.h"
#include "binlog_api.h"

#include <mysql_com.h>
#include <sql_common.h>

using namespace v8;

namespace zongji {

  namespace internal {

    using namespace std;

    uchar *net_store_data(uchar* destination, const uchar* source, size_t length)
    {
      destination = net_store_length(destination, length);
      memcpy(destination, source, length);
      return destination + length;
    }

    int sync_connect(MYSQL *mysql,
          const char* user, const char* password, const char* host,
          uint port, long offset) {

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
      if (!mysql_real_connect(mysql, host, user, password, "", port, 0, 0))
        return ERR_FAIL;

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
        It sends a command packet to the mysql-server.

        @retval ERR_OK      if success
        @retval ERR_FAIL    on failure
      */
      if (simple_command(mysql, COM_REGISTER_SLAVE, buf, (size_t) (pos - buf), 0))
        return ERR_FAIL;

      return ERR_OK;
    }

    char* to_cstring(Local<String> value) {
      String::Utf8Value theString(value);
      return *theString;
    }

    int Connection::connect(const char* user, const char* password,
                  const char* host, uint port, size_t offset) {

      m_mysql= mysql_init(NULL);

      if (!m_mysql)
        return ERR_FAIL;

      int err = sync_connect(m_mysql, user, password, host, port, offset);

      if (err != ERR_OK)
        return err;

      // const char* binlog_file = "";
      // start_binlog_dump(binlog_file, offset);
      return ERR_OK;
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
      const char* user = internal::to_cstring(args[0]->ToString());
      const char* password = internal::to_cstring(args[1]->ToString());
      const char* host = internal::to_cstring(args[2]->ToString());
      uint port = args[3]->Uint32Value();

      Connection* conn = ObjectWrap::Unwrap<Connection>(args.This());
      conn->m_connection->connect(user, password, host, port);
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
