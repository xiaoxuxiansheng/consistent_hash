# redmq
<p align="center">
<img src="https://github.com/xiaoxuxiansheng/consistent_hash/blob/main/img/frame.png" height="400px/"><br/><br/>
<b>consistent_hash: 纯 golang 实现的一致性哈希算法模块</b>
<br/><br/>
</p>

## 💡 `一致性哈希` 技术原理与实现解析
使用此 sdk 进行实践前，建议先行了解与一致性哈希有关的理论知识，做到知行合一<br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484641&idx=1&sn=764f69ab47ba7b3450f6300fde4f34a5">理论篇：一致性哈希算法原理解析</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484687&idx=1&sn=36befe944baf5a8314f7dc575c21248a">实战篇：从零到一落地实现一致性哈希算法(待补充)</a> <br/><br/>

## 🖥 使用
- 构造 redis 客户端实例<br/><br/>
```go
import "github.com/xiaoxuxiansheng/redmq/redis"
func main(){
    redisClient := redis.NewClient("tcp","my_address","my_password")
    // ...
}
```

## 🐧 使用示例
使用示例代码可以参见 ./example_test.go：
