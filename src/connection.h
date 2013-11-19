#ifndef ZONGJI_CONNECTION_H_
#define ZONGJI_CONNECTION_H_

#include <node.h>
#include <v8.h>

namespace zongji {

  using namespace v8;

  class Connection : public node::ObjectWrap {
  public:
    static Handle<Value> NewInstance(const Arguments& args);

  private:
    static Handle<Value> New(const Arguments& args);
    static Handle<Value> WaitForNextEvent(const Arguments& args);
  };
}

#endif
