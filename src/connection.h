#ifndef ZONGJI_CONNECTION_H_
#define ZONGJI_CONNECTION_H_

#include <node.h>
#include <v8.h>

#include <my_global.h>
#include <mysql.h>
#ifdef min //definition of min() and max() in std and libmysqlclient
           //can be/are different
#undef min
#endif
#ifdef max
#undef max
#endif

#define MAX_PACKAGE_SIZE 0xffffff

namespace zongji {

  using namespace v8;

  namespace internal {

    class Connection {

    public:
      Connection() {
        // constructor of zongji::internal::Connection
      }

      ~Connection() {
        delete m_mysql;
      }

      bool connect(const char* user, const char* password,
                  const char* host, uint port, size_t offset = 4);

      const char* m_error;

    private:
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
    static Handle<Value> New(const Arguments& args);
    static Handle<Value> Connect(const Arguments& args);
    static Handle<Value> WaitForNextEvent(const Arguments& args);

    internal::Connection* m_connection;
  };
}

#endif
