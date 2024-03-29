<?php

namespace Es;

use Es\Builder\Connection;
use Es\Builder\Query;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\ConfigInterface;

/**
 * Class Es
 * @package ckales
 * @method \Es\Builder\Query index(string $index) static 索引，对应的mysql的数据表
 * @method \Es\Builder\Query search() 自行组装查询条件进行列表查询
 * @method \Es\Builder\Query first() 查询单条数据
 * @method \Es\Builder\Query get() 查询多条数据
 * @method \Es\Builder\Query paginate($page_size = 10, $page = 1) 分页器
 * @method \Es\Builder\Query whereOr($map = []) or查询条件，与一般的whereor链式查询规则不一样，格式及注意事项请参考README.md文档
 * @method \Es\Builder\Query where($map = []) 查询条件，格式请参考README.md文档
 * @method \Es\Builder\Query offset($num = 0) 起始位置
 * @method \Es\Builder\Query limit($num = 0) 查询条数
 * @method \Es\Builder\Query highlight($fields = []) 字段高亮，格式['title', 'name']
 * @method \Es\Builder\Query order($order = []) 排序方式,格式['_score' => 'desc', 'create_at' => 'asc']
 * @method \Es\Builder\Query count() 对应mysql的count方法
 * @method \Es\Builder\Query sum($field = '') 对应mysql的sum方法
 * @method \Es\Builder\Query groupAggs($aggs = [], $group_by_field = '') static 对count(*) group by 进行封装，对应mysql select count()，sum() from table group by {$group_by_field}
 * @method \Es\Builder\Query having($aggs = []) 对应mysql的having方法
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