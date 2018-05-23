# rabbitUtil
rabbitMQ包裹函数

# 安装方法
```shell
go get -u -v -insecure github.com/leeming87v5/rabbitUtil
```
# 示例代码
## PrefetchConsumer
用于从指定的 rabbitMQ 队列中取出数据，并且使用自定义的回调函数处理之。
```go
package main

import "fmt"
import "os"
import "github/leeming87v5/rabbitUtil"
import "github/leeming87v5/rabbitUtil/PrefetchConsumer"

func main() {
  // 首先，初始化一个 prefetch consumer 对象
  // First, init a prefetch consumer object
  hosts := []string{"amqp://guest:guest@127.0.0.1:5672"}
  queueName := "your_test_queue"
  consumer, err := PrefetchConsumer.NewPrefetchConsumer(hosts, queueName)
  if err != nil {
	  fmt.Printf("[%v] prefetch consume init error: %v\n", time.Now(), err)
	  os.Exit(1)
  }
  
  // 然后，调用 consumer.Consume()，指定回调函数，提供该回调函数需要用到的任意参数。
  // Second, call consumer.Consume() with your own callback function together with it's parameters.
  consumer.Consume(myCallback, 1234, "testString")
}

// 在此定义你的回调函数，第一个参数从 rabbitMQ 中接收到的消息体，会自动被填充，无需担心。
// 后面的这个 args 参数需要你按需转换成特定的类型，下面的参考代码中给出了转换的示例。
// 若该函数的返回值不为 nil，则会自动触发对该消息的 nack 逻辑，因此，如果可以正常处理完该消息，返回 nil 即可。
// your callback comes here
func myCallback(message []byte, args ...interface{}) error {
  var realArgs []interface{} = args[0].([]interface{})
  arg0 := realArgs[0].(int)
  arg1 := realArgs[1].(string)
  
  fmt.Fprintf(os.Stdout, "what i got from rabbitMQ is: %v\n", string(message))
  fmt.Fprintf(os.Stdout, "what i got from args are: %v, %v\n", arg0, arg1)
  
  // return errors.New("I should fail")
  return nil
}
```
