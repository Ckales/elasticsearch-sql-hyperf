# elasticsearch-sql-hyperf
在hyperf中使用sql链式操作格式进行elasticsearch操作，PHP版本7.4及以上，Elasticsearch版本7.8及以上

# 安装

```php
composer require chingli/elasticsearch-sql-hyperf
```

# 依赖

```bash

"require": {
    "php": ">=7.4",
    "hyperf/elasticsearch": "3.0.*",
}

```

# 配置

hyperf项目下→config目录→autoload目录，新建elasticsearch.php配置文件，文件内容为下：

```php
<?php

declare(strict_types=1);

return [
    'default' => [
        'host' 		=> 'xxx', // es地址
        'user' 		=> 'xxx', // 用户名
        'password' 	=> 'xxx', // 密码
        'retries'	=> 5, // 重试次数
    ],
];

```

