# elasticsearch-sql-hyperf
在hyperf中使用sql链式操作格式进行elasticsearch操作，PHP版本7.4及以上，Elasticsearch版本7.8及以上

# 安装

```php
composer require ckales/elasticsearch-sql-hyperf
```

# 依赖

```bash

"require": {
    "php": ">=7.4",
    "hyperf/elasticsearch": "3.0.*",
    "hyperf/logger": "^3.0"
}

```

# 配置

hyperf项目下→config目录→autoload目录，新建elasticsearch.php配置文件，文件内容为下：

```php
<?php

declare(strict_types=1);

return [
    'default' => [
        'host' 		    => 'xxx', // es地址
        'user' 		    => 'xxx', // 用户名
        'password' 	    => 'xxx', // 密码
        'retries'	    => 5, // 重试次数
        'debug'	        => false, // 调试模式，true时会输入转换后es搜索格式
        'log_config'    => default, // hyperf日志配置名称，config/autoload/logger.php中配置
    ],
];

```

# 使用方法

```php
<?php

use Es\Es;

// 完全匹配上才可以返回结果
$map = [
    ['status', '=', 1],
    ['title', 'like', '搜索%'],
    ['create_at', 'between', [date('Y-m-d H:i:s', time() - 86400), date('Y-m-d H:i:s')]]
];

// 必须匹配到其中一个
$region_map = [
    ['province', '=', '安徽'],
    ['province', 'in', ['湖北', '上海']],
];

// 必须匹配到其中一个
$keyword_map = [
    ['show_tag', 'like', '%钢铁%'],
    ['hide_tag', 'like', '%材料%'],
];

Es::index('索引（类似mysql表名）')
    ->where($map)
    ->whereOr($region_map)
    ->whereOr($keyword_map)
    ->paginate();

```

