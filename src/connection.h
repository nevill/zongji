#ifndef ZONGJI_CONNECTION_H_
#define ZONGJI_CONNECTION_H_

#include <node.h>
#include <node_version.h>
#include <node_buffer.h>

#include <v8.h>

#include <my_global.h>
#include <mysql.h>

#ifdef min //definition of min() and max() in std and libmysqlclient
           //can be different
#undef min
#endif
#ifdef max
#undef max
#endif

namespace zongji {

  using namespace v8;

  namespace internal {

    class Connection {

    public:
      Connection() : m_mysql() {
        // constructor of zongji::internal::Connection
      }

      ~Connection() {
        delete m_mysql;
      }

      bool connect(const char* user, const char* password,
                  const char* host, uint port);

      bool beginBinlogDump(size_t offset = 4);
      int nextEvent();

      const char* m_error;
      MYSQL *m_mysql;
    };
  }

  class Connection : public node::ObjectWrap {
  public:
    Connection() : node::ObjectWrap(),
                   m_connection(new internal::Connection()) {
      // constructor of zongji::Connection
    }

    ~Connection() {
      delete m_connection;
    }

    static Handle<Value> NewInstance(const Arguments& args);

  private:
    internal::Connection* m_connection;

    static Handle<Value> New(const Arguments& args);
    static Handle<Value> Connect(const Arguments& args);
    static Handle<Value> BeginBinlogDump(const Arguments& args);
    static Handle<Value> WaitForNextEvent(const Arguments& args);

    static void fetchNextEvent(uv_work_t* req);
    static void afterFetchNextEvent(uv_work_t* req, int status);
    struct EventRequest {
      bool hasError;
      char* eventBuffer;
      int bufferLength;
      Persistent<Function> callback;
      Connection* conn;
    };
  };
}

#endif
