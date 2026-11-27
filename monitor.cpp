// Монитор: поток-поставщик и поток-потребитель (C++17).
//
// Сборка: g++ -std=c++17 -Wall -Wextra -pthread monitor.cpp -o monitor

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

// Событие — произвольный (в том числе полиморфный) объект.
// Сериализация не нужна: между потоками передаётся только указатель,
// а unique_ptr однозначно передаёт владение от поставщика к потребителю.
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

// Монитор: разделяемое состояние, мьютекс и условные переменные вместе.
class Monitor
{
public:
  // Поставщик. Блокируется (без нагрузки на CPU), пока предыдущее
  // событие не забрано потребителем.
  void provide(std::unique_ptr<Event> event)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    slotFree_.wait(lock, [this] { return !ready_; });

    data_ = std::move(event);
    ready_ = true;
    // Сообщение печатается под мьютексом, до того как потребитель
    // сможет получить событие, — порядок вывода не нарушается.
    std::cout << "[поставщик]   отправлено " << data_->describe() << std::endl;
    eventReady_.notify_one();
  }

  // Потребитель. Возвращает nullptr, если работа завершена
  // и необработанных событий больше нет.
  std::unique_ptr<Event> consume()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // wait с предикатом — это цикл while(!pred) wait(): защита от
    // ложных пробуждений и от уведомления, пришедшего раньше ожидания.
    eventReady_.wait(lock, [this] { return ready_ || stopping_; });

    if (!ready_)               // stopping_ == true и событий нет
      return nullptr;

    std::unique_ptr<Event> event = std::move(data_);
    ready_ = false;
    std::cout << "[потребитель] получено   " << event->describe() << std::endl;
    slotFree_.notify_one();
    return event;
  }

  // Разбудить всех ожидающих и сообщить о завершении.
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
  std::condition_variable eventReady_;   // поставщик -> потребитель
  std::condition_variable slotFree_;     // потребитель -> поставщик
  std::unique_ptr<Event> data_;          // разделяемые данные
  bool ready_ = false;                   // предикат: событие готово
  bool stopping_ = false;                // предикат: завершение работы
};

int main()
{
  constexpr int eventsCount = 5;
  Monitor monitor;

  std::thread consumer([&monitor] {
    while (auto event = monitor.consume())
    {
      // Здесь можно выполнять долгую обработку: мьютекс уже свободен.
    }
  });

  std::thread producer([&monitor] {
    for (int i = 1; i <= eventsCount; ++i)
    {
      std::this_thread::sleep_for(std::chrono::seconds(1));  // вне мьютекса
      monitor.provide(std::make_unique<TextEvent>(i, "данные №" + std::to_string(i)));
    }
    monitor.stop();
  });

  producer.join();
  consumer.join();
  return 0;   // mutex и condition_variable уничтожаются деструкторами
}
