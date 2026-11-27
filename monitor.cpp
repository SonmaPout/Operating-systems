#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

class Event
{
public:
  virtual ~Event() = default;
  virtual std::string describe() const = 0;
};

class TextEvent : public Event
{
public:
  TextEvent(int id, std::string text) : id_(id), text_(std::move(text)) {}
  int id() const { return id_; }
  std::string describe() const override
  {
    return "событие " + std::to_string(id_) + ": \"" + text_ + "\"";
  }
private:
  int id_;
  std::string text_;
};

class Monitor
{
public:
  void provide(std::unique_ptr<Event> event)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    slotFree_.wait(lock, [this] { return !ready_; });

    data_ = std::move(event);
    ready_ = true;
    std::cout << "[поставщик]   отправлено " << data_->describe() << std::endl;
    eventReady_.notify_one();
  }

  std::unique_ptr<Event> consume()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    eventReady_.wait(lock, [this] { return ready_ || stopping_; });

    if (!ready_)              
      return nullptr;

    std::unique_ptr<Event> event = std::move(data_);
    ready_ = false;
    std::cout << "[потребитель] получено   " << event->describe() << std::endl;
    slotFree_.notify_one();
    return event;
  }


  void stop()
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    eventReady_.notify_all();
  }

private:
  std::mutex mutex_;
  std::condition_variable eventReady_;   
  std::condition_variable slotFree_;    
  std::unique_ptr<Event> data_;         
  bool ready_ = false;                  
  bool stopping_ = false;               
};

int main()
{
  constexpr int eventsCount = 5;
  Monitor monitor;

  std::thread consumer([&monitor] {
    while (auto event = monitor.consume())
    {
    }
  });

  std::thread producer([&monitor] {
    for (int i = 1; i <= eventsCount; ++i)
    {
      std::this_thread::sleep_for(std::chrono::seconds(1));  
      monitor.provide(std::make_unique<TextEvent>(i, "данные №" + std::to_string(i)));
    }
    monitor.stop();
  });

  producer.join();
  consumer.join();
  return 0;   
}
