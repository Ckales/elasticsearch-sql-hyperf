<?php

namespace Es;

use Es\Builder\Connection;
use Es\Builder\Query;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\ConfigInterface;

/**
 * Class Es
 * @package ckales
 * @method \Es\Builder\Query index(string $index) static 索引，对应的mysql的表
 * @method \Es\Builder\Query search() static 从主服务器读取数据
 * @method \Es\Builder\Query select() static 从主服务器读取数据
 * @method \Es\Builder\Query paginate($page_size = 10, $page = 1) static 从主服务器读取数据
 * @method \Es\Builder\Query find() static 从主服务器读取数据
 * @method \Es\Builder\Query whereOr($map = []) static 从主服务器读取数据
 * @method \Es\Builder\Query where($map = []) static 从主服务器读取数据
 * @method \Es\Builder\Query offset($num = 0) static 从主服务器读取数据
 * @method \Es\Builder\Query limit($num = 0) static 从主服务器读取数据
 *  @method \Es\Builder\Query highlight($fields = []) static 从主服务器读取数据
 * @method \Es\Builder\Query order($order = []) static 从主服务器读取数据
 */
class Es
{
    /**
     * 当前es连接对象
     */
    protected static $client;

    private function __clone()
    {
        // 防止克隆
    }

    public function __wakeup()
    {
        // 防止反序列化
    }

    /**
     * 连接es
     * @param $config_name
     * @return Query
     */
    public static function connect($config_name = 'default')
    {

        $container = ApplicationContext::getContainer();
        $config = $container->get(ConfigInterface::class)->get("elasticsearch.{$config_name}");

        if(empty($config)){
            throw new \Exception('Elasticsearch config not found');
        }

        self::$client = Connection::instance($config);

        return new Query(self::$client, $config);
    }

    /**
     * 静态化访问
     * @param $method
     * @param $args
     * @return mixed
     * @author 李静
     */
    public static function __callStatic($method, $args)
    {
        return call_user_func_array([static::connect(), $method], $args);
    }
}