<?php

namespace Es\Builder;

use Hyperf\Context\ApplicationContext;
use Hyperf\Elasticsearch\ClientBuilderFactory;
use Hyperf\Contract\ConfigInterface;

abstract class Connection
{
    protected static $instance = [];

    // 连接参数配置
    protected $config = [
        // 服务器地址
        'host'        => '',
        // 用户名
        'user'        => '',
        // 密码
        'password'        => '',
        // 重试次数
        'retries'      => 5,
    ];

    /**
     * 初始化
     * @access public
     */
    public function __construct()
    {
        // 执行初始化操作
        $this->initialize();
    }

    /**
     * 初始化
     * @access protected
     * @return void
     */
    protected function initialize()
    {}

    /**
     * 取得es连接实例
     * @access public
     * @return Connection
     */
    public static function instance($config_name = 'default')
    {

        $container = ApplicationContext::getContainer();
        $config = $container->get(ConfigInterface::class)->get("elasticsearch.{$config_name}");

        $container = ApplicationContext::getContainer();
        // 如果在协程环境下创建，则会自动使用协程版的 Handler，非协程环境下无改变
        $builder = $container->get(ClientBuilderFactory::class)->create();

        self::$instance = $builder->setSSLVerification(false)
            ->setBasicAuthentication($config['user'], $config['password'])
            ->setHosts([$config['host']])
            ->setConnectionPool('\Elasticsearch\ConnectionPool\SimpleConnectionPool', [])
            ->setRetries($config['retries'])
            ->build();

        return self::$instance;
    }

}
