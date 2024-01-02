<?php

namespace Es;

use Es\Builder\Connection;
use Es\Builder\Query;

/**
 * Class Es
 * @package chingli
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

    public static function connect($config_name = 'default')
    {

        self::$client = Connection::instance($config_name);

        return new Query(self::$client);
    }

    public static function __callStatic($method, $args)
    {
        return call_user_func_array([static::connect(), $method], $args);
    }
}