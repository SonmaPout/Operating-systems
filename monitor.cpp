#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using namespace std;
class Event{
public:
  virtual ~Event() = default;
  virtual string describe() const = 0;
};

class TextEvent : public Event{
public:
  TextEvent(int id, string text) : id_(id), text_(move(text)) {}
  int id() const { return id_; }
  string describe() const override{
    return "событие " + to_string(id_) + ": \"" + text_ + "\"";
  }
private:
  int id_;
  string text_;
};

class Monitor{
public:
  void provide(unique_ptr<Event> event){
    unique_lock<mutex> lock(mutex_);
    slotFree_.wait(lock, [this] { return !ready_; });

    data_ = move(event);
    ready_ = true;
    cout << "[поставщик]   отправлено " << data_->describe() << endl;
    eventReady_.notify_one();
  }

  unique_ptr<Event> consume(){
    unique_lock<mutex> lock(mutex_);
    eventReady_.wait(lock, [this] { return ready_ || stopping_; });

    if (!ready_)              
      return nullptr;

    unique_ptr<Event> event = move(data_);
    ready_ = false;
    cout << "[потребитель] получено   " << event->describe() << endl;
    slotFree_.notify_one();
    return event;
  }

  void stop(){
    {
      lock_guard<mutex> lock(mutex_);
      stopping_ = true;
    }
    eventReady_.notify_all();
  }

private:
  mutex mutex_;
  condition_variable eventReady_;   
  condition_variable slotFree_;   
  unique_ptr<Event> data_;        
  bool ready_ = false;                  
  bool stopping_ = false;               
};

int main(){
  constexpr int eventsCount = 5;
  Monitor monitor;
  thread consumer([&monitor] {
    while (auto event = monitor.consume()){}
  });

  thread producer([&monitor] {
    for (int i = 1; i <= eventsCount; ++i){
      this_thread::sleep_for(chrono::seconds(1));
      monitor.provide(make_unique<TextEvent>(i, "данные №" + to_string(i)));
    }
    monitor.stop();
  });
  producer.join();
  consumer.join();
  return 0;  
}
