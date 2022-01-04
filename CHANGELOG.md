# Changelog

## Version 1.2.1

- 针对TDSQL的Proxy协议进行了适配，修复了client deprecate eof问题；

## Version 1.2.0

- 优化Load Balance逻辑，同时支持连接收敛和非收敛两种实现；
- 优化了负载均衡策略算法，并对它们进行了合并，保留一种负载均衡算法SED；
- 加入了主动Heartbeat功能，与Blacklist功能结合优化高可用功能的实现逻辑；
- 修改了驱动名和URL串的格式；

## Version 1.1.0

- 新增了 SED 负载均衡算法
- 新增了 NQ 负载均衡算法
- 补充完善了测试用例

## Version 1.0.0

- 增加了单独编译打包配置，简化编译打包流程；
- 修改了部分编译参数；
- 重构了程序包名；
- 简化了部分程序结构；
- 重新启用新版本号；