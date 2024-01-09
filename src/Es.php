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
 * @method \Es\Builder\Query search() static 自行组装查询条件进行列表查询
 * @method \Es\Builder\Query select() static 查询多条数据
 * @method \Es\Builder\Query find() static 查询单条数据
 * @method \Es\Builder\Query paginate($page_size = 10, $page = 1) static 分页器
 * @method \Es\Builder\Query whereOr($map = []) static or查询条件，格式及注意事项请参考README.md文档
 * @method \Es\Builder\Query where($map = []) static 查询条件，格式请参考README.md文档
 * @method \Es\Builder\Query offset($num = 0) static 起始位置
 * @method \Es\Builder\Query limit($num = 0) static 查询条数
 * @method \Es\Builder\Query highlight($fields = []) static 字段高亮，格式['title', 'name']
 * @method \Es\Builder\Query order($order = []) static 排序方式,格式['_score' => 'desc', 'create_at' => 'asc']
 * @method \Es\Builder\Query groupCount($field = '') static 对count($field) group by $field 进行封装，对应mysql select count({$field}) from table group by {$field}
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