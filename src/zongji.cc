#include "connection.h"
using namespace v8;

namespace zongji {
  Handle<Value> Connection::NewInstance(const Arguments& args) {
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
    tpl->SetClassName(String::NewSymbol("Connection"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    tpl->PrototypeTemplate()->Set(String::NewSymbol("waitForNextEvent"),
                                  FunctionTemplate::New(WaitForNextEvent)->GetFunction());

    Persistent<Function> constructor = Persistent<Function>::New(tpl->GetFunction());
    return scope.Close(constructor->NewInstance(0, NULL));
  }

  Handle<Value> Connection::WaitForNextEvent(const Arguments& args) {
    HandleScope scope;
    return scope.Close(Undefined());
  }

  Handle<Value> Connection::New(const Arguments& args) {
    HandleScope scope;

    Connection* obj = new Connection();
    obj->Wrap(args.This());
    return args.This();
  }
}

Handle<Value> Connect(const Arguments& args) {
  HandleScope scope;
  return scope.Close(zongji::Connection::NewInstance(args));
}

void init(Handle<Object> exports) {
  exports->Set(String::NewSymbol("connect"),
               FunctionTemplate::New(Connect)->GetFunction());
}

NODE_MODULE(zongji, init)
