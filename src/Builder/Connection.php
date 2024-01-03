<?php

namespace Es\Builder;

use Hyperf\Context\ApplicationContext;
use Hyperf\Elasticsearch\ClientBuilderFactory;
use Hyperf\Contract\ConfigInterface;

abstract class Connection
{
    protected static $instance = [];

    // 连接参数配置
    protected static $config = [
        'host'          => '', // 服务器地址
        'user'          => '', // 用户名
        'password'      => '', // 密码
        'retries'       => 5, // 重试次数
        'debug'	        => false, // 调试模式
        'log_config'    => 'default', // hyperf日志配置名称，config/autoload/logger.php中配置
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
     * @param $config 参数配置
     * 取得es连接实例
     * @return \Elasticsearch\Client
     */
    public static function instance($config = [])
    {
        if (!empty($config)) {
            self::$config = array_merge(self::$config, $config);
        }

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
