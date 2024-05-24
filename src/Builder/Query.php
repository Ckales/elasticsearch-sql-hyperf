<?php

namespace Es\Builder;

use Elasticsearch\Client;
use Es\Es;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Psr\Log\LoggerInterface;
use Hyperf\Logger\LoggerFactory;

class Query
{

    /**
     * 当前es连接对象
     * @var Client
     */
    protected $client;

    /**
     * @var array 参数配置
     */
    private $options;

    protected LoggerInterface $logger;

    public function __construct(Client $client = null, $config = [])
    {
        if (is_null($client)) {
            $this->client = Es::connect();
        } else {
            $this->client = $client;
        }

        $this->logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('elasticsearch-sql-hyperf', $config['log_config'] ?? 'default');

        $this->options = [];
        $this->options['debug'] = $config['debug'] ?? false;
    }

    /**
     * 设置索引
     * @param string $index
     * @return $this
     */
    public function index($index = '')
    {
        $this->options['index'] = $index;
        return $this;
    }

    /**
     * 判断数据是否存在
     * @param int $id 文档id
     * @param string $index 索引
     * @return bool
     */
    public function exists(int $id = 0, string $index = '')
    {
        try {
            $index && $this->options['index'] = $index;
            if($id) {
                $params = [
                    'id' => $id,
                    'index' => $this->options['index'],
                ];
                return $this->client->exists($params);
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return false;
    }

    /**
     * 批量插入数据
     * @param array $list
     * @param string $index
     * @return array
     */
    public function batchIndex(array $list = [], string $index = '')
    {
        try {
            $index && $this->options['index'] = $index;
            if(!empty($list)) {
                $params = ['body'=>[]];
                foreach ($list as $data) {
                    $id = $data['id'] ?? 0;
                    $params['body'][] = [
                        'index' => [     //注意这里的动作为index, 表示库中有就更新，没有就创建
                            '_index' => $this->options['index'],   //注意这里的字段前缀是_
                            '_id'    => $id
                        ]
                    ];
                    $params['body'][] = $data;
                }

                $result = $this->client->bulk($params);
                unset($params);
                return $result;
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 自行组装查询条件进行列表查询
     * @param array $query
     * @param string $index
     * @return array|callable
     */
    public function search(array $query = [], string $index = '')
    {
        try {

            if(!empty($this->options['debug'])){
                $this->logger->debug('最终请求参数==========' . json_encode($query, JSON_UNESCAPED_UNICODE));
            }

            $index && $this->options['index'] = $index;
            $params = [
                'index' => $this->options['index'],
                'body' => $query
            ];

            return $this->client->search($params);

        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 单条数据查询
     * @return array
     */
    public function first()
    {
        try {

            if(!empty($this->options['group_key'])){
                // group查询获取单条数据
                $group_get = $this->_group_get();
                $result = $group_get[0] ?? [];
            }else{
                // 普通查询获取单条数据
                $result = $this->_first();
            }

            return $result;

        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 普通查询获取单条数据
     * @return array
     * @throws \Exception
     */
    private function _first()
    {
        $this->options['from'] = 0;
        $this->options['size'] = 1;

        $result = $this->_search();

        return $result['hits']['hits'][0]['_source'] ?? [];
    }

    /**
     * 多条数据查询
     * es原生查询规则，默认查询10条，如果需要查询更多，请链式limit()方法或直接在get中传入limit
     * @return array
     */
    public function get($limit = 0)
    {
        try {

            if(!empty($this->options['group_key'])){
                $group_get = $this->_group_get();
                $result = array_slice($group_get, 0, $limit);
            }else{
                $result = $this->_get($limit);
            }

            return $result;

        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 多条数据查询
     * @param $limit
     * @return array
     * @throws \Exception
     */
    private function _get($limit = 0)
    {
        $limit = intval($limit);
        if($limit > 0){
            $this->options['size'] = $limit;
        }

        $result = $this->_search();

        if(!empty($result['hits']['hits'])){
            $list = array_column($result['hits']['hits'], '_source');
        }else{
            $list = [];
        }
        return $list;
    }

    /**
     * group查询
     * @return array
     */
    private function _group_get()
    {
        // having 处理
        if(!empty($this->options['having'])){
            $this->options['group_param']['aggs'][$this->options['group_key']]['aggs']['having'] = $this->options['having'];
        }

        // 进行数据查询
        $res = $this->search($this->options['group_param']);
        $res = $res['aggregations'][$this->options['group_key']]['buckets'] ?? [];

        // 对查询到的数据进行格式化处理
        $data = [];
        foreach ($res as $key => $value) {
            $tmep_data = [
                $this->options['group_by_field'] => $value['key'],
            ];

            foreach ($this->options['group_aggs'] as $item) {
                $tmep_data[$item[2]] = $value[$item[2]]['value'];
            }

            $data[] = $tmep_data;
        }

        return $data;
    }

    /**
     * 分页查询
     * @param $page_size int 分页偏移量
     * @param $page int 分页页码
     * @return array
     */
    public function paginate($page_size = 10, $page = 1)
    {
        try {
            // 查询条件组装
            $page = max($page, 1);
            $this->options['from'] = ($page - 1) * $page_size;
            $this->options['size'] = $page_size;

            // 数据查询
            $result = $this->_search();

            $total = $result['hits']['total']['value'] ?? 0;

            if(!empty($result['hits']['hits'])){
                $list = array_column($result['hits']['hits'], '_source');
            }else{
                $list = [];
            }

            // 将数据组装成分页数据
            $data = $this->_imitate_page($total,$page_size, $page);
            $data['data'] = $list;

        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
            // 异常时模拟分页数据，正常返回分页格式
            $data = $this->_imitate_page(0,$page_size, $page);
        }
        return $data;
    }



    /**
     * 模拟分页
     * @param $total int 总条数
     * @param $limit int 分页偏移量
     * @param $current_page int 当前页码
     * @return array
     */
    private function _imitate_page($total, $limit, $current_page)
    {
        if ($total <= 0){
            return [
                'total' => 0,
                'per_page' => intval($limit),
                'current_page' => 1,
                'last_page' => 0,
                'data' => []
            ];
        }

        return [
            'total' => $total,
            'per_page' => intval($limit),
            'current_page' => intval($current_page),
            'last_page' => ceil($total / $limit),
            'data' => []
        ];
    }

    /**
     * where or查询
     * 注意！！！当多个不同的whereOR条件查询时，请使用多个whereOr进行拼接！！！
     * ！！！例如：MYSQL中 where (id = '1' or id = '2') and (name = '3' or title = '4')！！！
     * ！！！请使用->whereOr([['id', '=', '1'], ['id', '=', '2']])->whereOr([['name', '=', '3'], ['title', '=', '4']])进行拼接！！！
     * @param array $map 查询条件
     * @return $this
     */
    public function whereOr($map = [])
    {
        $query_list = $this->whereParse($map);
        if(!empty($query_list)){
            $this->options['query']['bool']['must'][] = [
                "bool" => [
                    "minimum_should_match" => 1,
                    "should" => $query_list
                ]
            ];
        }

        return $this;
    }

    /**
     * 查询参数格式化
     * @param $map array 查询条件
     * @return $this
     */
    public function where($map = [])
    {
        $query_list = $this->whereParse($map);
        if(!empty($query_list)){
            $this->options['query']['bool']['must'] = $query_list;
        }

        return $this;
    }

    /**
     * 查询条件格式化
     * @param $map array 查询条件
     * @return array
     * @throws \Exception
     */
    public function whereParse($map = [])
    {
        $query_list = [];
        foreach ($map as $item) {
            if(count($item) != 3){
                Throw new \Exception("格式错误");
            }
            switch ($item[1]) {
                case '=':
                    $query_list[] = [
                        "term" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case '>':
                    $query_list[] = [
                        "range" => [
                            $item[0] => [
                                "gt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '>=':
                    $query_list[] = [
                        "range" => [
                            $item[0] => [
                                "gte" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<':
                    $query_list[] = [
                        "range" => [
                            $item[0] => [
                                "lt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<=':
                    $query_list[] = [
                        "range" => [
                            $item[0] => [
                                "lte" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case 'between':
                    if(count($item[2])!=2){
                        Throw new \Exception("格式between错误");
                    }
                    $query_list[] = [
                        "range" => [
                            $item[0] => [
                                "gte" => $item[2][0],
                                "lte" => $item[2][1]
                            ]
                        ]
                    ];
                    break;
                case 'in':
                    if(!is_array($item[2])){
                        $item[2] = explode(',', $item[2]);
                    }
                    $query_list[] = [
                        "terms" => [
                            $item[0] => array_values($item[2])
                        ]
                    ];
                    break;
                case 'like':
                    $query_list[] = [
                        "match_phrase" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;
                case 'vague_like':
                    $query_list[] = [
                        "match" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;

                default:
                    Throw new \Exception("格式错误");
            }
        }

        return $query_list;
    }

    /**
     * 查询参数格式化
     * @param $fields
     * @return $this
     */
    public function whereNotNull($fields = [])
    {
        foreach ($fields as $item) {
            $this->options['query']['bool']['must'][] = [
                "exists" => [
                    'field' => $item
                ]
            ];
        }

        return $this;
    }

    /**
     * 查询参数格式化
     * @param $fields
     * @return $this
     */
    public function whereNull($fields = [])
    {
        foreach ($fields as $item) {
            $this->options['query']['bool']['must_not'][] = [
                "exists" => [
                    'field' => $item
                ]
            ];
        }

        return $this;
    }

    /**
     * 查询字段
     * @param $fileds array
     * @return $this
     */
    public function select($fileds = [])
    {
        if(!empty($fileds) && is_array($fileds)){
            $this->options['_source'] = $fileds;
        }

        return $this;
    }

    /**
     * 查询封装
     * @return array|callable
     * @throws \Exception
     */
    private function _search()
    {

        $body = [];

        // query条件组装
        if(!empty($this->options['query'])){
            $body["query"] = $this->options['query'];
        }

        // group条件组装
        if(!empty($this->options['group'])){
            $body['aggs'] = ['group_by' => $this->options['group']];
        }

        if(!empty($this->options['aggs'])){
            if(!empty($body['aggs'])){
                $body['aggs']['group_by']['aggs'] = $this->options['aggs'];
            }else{
                $body['aggs'] = $this->options['aggs'];
            }
        }

        // order条件组装
        $order = $this->options['order'] ?? [];
        if(!empty($order)){
            $body['sort'] = $order;
        }

        // 深度分页after查询
        $after_value = $this->options['_after_value'] ?? [];
        if(!empty($after_value)){
            if(!is_array($after_value)){
                throw new \Exception('after_value必须是数组');
            }
            if(empty($order) || count($after_value) != count($order)){
                throw new \Exception('after_value元素数量必须与order元素数量相同');
            }
            $body['search_after'] = $after_value;
        }

        // 查询起始位置
        if(isset($this->options['from'])){
            $body['from'] = $this->options['from'];
        }

        // 查询数量
        if(isset($this->options['size'])){
            $body['size'] = $this->options['size'];
        }

        // 高亮字符
        if(!empty($this->options['highlight'])){
            $body['highlight'] = $this->options['highlight'];
        }

        $params = [
            'index' => $this->options['index'],
            'body' => $body
        ];

        if(!empty($this->options['_source'])){
            $params['_source'] = $this->options['_source'];
        }

        if(!empty($this->options['debug'])){
            $this->logger->debug('最终请求参数==========' . json_encode($params, JSON_UNESCAPED_UNICODE));
        }
        $result = $this->client->search($params);

        if(!empty($this->options['debug'])){
//            $this->logger->debug('最终请求数据==========' . json_encode($result, JSON_UNESCAPED_UNICODE));
        }
        if(!empty($result['hits']['hits']) && !empty($body['highlight']['fields'])){
            $hightlight_field = array_keys($body['highlight']['fields']);

            foreach ($result['hits']['hits'] as $key => $value) {
                if(!empty($value['_source']) && !empty($value['highlight'])){
                    foreach ($hightlight_field as $item) {
                        if(isset($value['_source'][$item]) && !empty($value['highlight'][$item])){
                            $value['_source'][$item] = $value['highlight'][$item][0];
                        }
                    }
                }

                $result['hits']['hits'][$key] = $value;
            }
        }

        return $result;
    }

    /**
     * 对count(*) group by 进行封装
     * 对应mysql select count()，sum() from table group by {$group_by_field}
     * 请注意：
     * ！！！
     *  如果需要统计的字段是个数组，请勿使用，因为最终统计出的数量是数组的元素数量，而不是匹配的数量
     *  例如聚合查询中[['order_ids', 'count', 'total_count']],，如果order_ids是个数组，最终查询到的是order_ids的元素数量
     * ！！！
     * @param $aggs array 聚合查询，格式：
     * [
     *      ['字段', '聚合查询类型', '别名']
     *      ['num', 'sum', 'total_num']
     *      ['id', 'count', 'total_count']
     * ]
     * @param $group_by_field string group by字段
     * 支持链式order方法、imit方法
     * @return $this
     * @throws \Exception
     */
    public function groupAggs($aggs = [], $group_by_field = '')
    {
        if(empty($aggs) || empty($group_by_field)){
            throw new \Exception('group条件不能为空');
        }

        if(empty($this->options['query'])){
            throw new \Exception('query条件不能为空');
        }

        $this->options['group_key'] = 'group_num_key';
        $this->options['group_by_field'] = $group_by_field;
        $this->options['group_aggs'] = $aggs;

        $param = [
            'size'  => 0,
            "query" => $this->options['query'],
        ];

        $aggs_query = [
            $this->options['group_key']=>[
                "terms"=>["field"=>$group_by_field,]
            ]
        ];

        // 排序方式
        if(!empty($this->options['order'])){
            foreach ($this->options['order'] as $key => $item) {
                $temp_key = key($item);
                $aggs_query[$this->options['group_key']]['terms']['order'][$temp_key] = $item[$temp_key]['order'];
            }
        }

        // 筛选数量
        if(!empty($this->options['size'])){
            $aggs_query[$this->options['group_key']]['terms']['size'] = $this->options['size'];
        }

        foreach ($aggs as $item) {
            if(count($item) != 3){
                Throw new \Exception("格式错误");
            }

            if(!in_array($item[1], ['count', 'value_count', 'sum', 'avg'])){
                Throw new \Exception("查询类型错误");
            }
            if($item[1] == 'count'){
                $item['1'] = 'value_count';
            }

            $aggs_query[$this->options['group_key']]['aggs'][$item[2]] = [$item[1]=>["field"=>$item[0]]];
        }

        $param['aggs'] = $aggs_query;

        $this->options['group_param'] = $param;

        return $this;
    }

    /**
     * having方法
     * @param $aggs array 格式：
     * [
     *      ['group字段别名', '条件', '值']
     *      ['total_num', '>=', 10]
     *      ['total_count', '<', 5]
     * ]
     * @return $this
     * @throws \Exception
     */
    public function having($aggs = [])
    {
        $having_query = [];
        $script = [];
        foreach ($aggs as $item) {
            if(count($item) != 3){
                Throw new \Exception("having格式错误");
            }

            $having_query['bucket_selector']['buckets_path'][$item[0]] = $item[0];
            $script[] = "params.{$item[0]} {$item[1]} {$item[2]}";
        }
        if(!empty($script)){
            $having_query['bucket_selector']['script']['source'] = implode(' && ', $script);
            $this->options['having'] = $having_query;
        }

        return $this;
    }

    /**
     * 排序方式
     * 格式['_score' => 'desc', 'create_at' => 'asc']
     * @param $order
     * @return $this
     */
    public function order($order = [])
    {
        $order_list = [];
        foreach ($order as $key => $value) {
            $order_list[][$key]['order'] = $value;
        }

        $this->options['order'] = $order_list;
        return $this;
    }

    /**
     * 起始位置
     * @param $num
     * @return $this
     */
    public function offset($num = 0)
    {
        $this->options['from'] = $num;

        return $this;
    }

    /**
     * 查询条数
     * @param $num
     * @return $this
     */
    public function limit($num = 0)
    {
        $this->options['size'] = $num;

        return $this;
    }

    /**
     * search_after 深分页
     * @param $after_value array 深分页值，需结合order使用，元素数量与order数量保持一致
     * @return $this
     */
    public function search_after($after_value = [])
    {
        $this->options['_after_value'] = $after_value;

        return $this;
    }

    /**
     * 字段高亮
     * 格式['title', 'name']，数组元素为需要高亮的字段
     * @param $fields array 需要高亮的字段
     * @param $pre_tags string 高亮前缀
     * @param $post_tags string 高亮后缀
     * @return $this
     */
    public function highlight($fields = [], $pre_tags = '', $post_tags = '')
    {
        // 自定义高亮格式
        if(empty($pre_tags) || empty($post_tags)){
            $pre_tags = '<span style="color: red;">';
            $post_tags = '</span>';
        }

        if(!empty($fields)){
            $highlight_fields = [];
            foreach ($fields as $item) {
                $highlight_fields[$item] = new \stdClass();
            }

            $highlight = [
                "order"  => "score",
                "boundary_scanner" => "sentence",
                'number_of_fragments'   => 0,
                "pre_tags" => [$pre_tags],
                "post_tags" => [$post_tags],
                "fields" => $highlight_fields,
            ];

            $this->options['highlight'] = $highlight;
        }

        return $this;
    }

    /**
     * 聚合count查询
     * @return int
     */
    public function count()
    {
        try {
            if(empty($this->options['query'])){
                throw new \Exception('query条件不能为空');
            }

            $body = [
                "query" => $this->options['query'],
            ];
            $params = [
                'index' => $this->options['index'],
                'body' => $body
            ];
            $this->options['query'] = [];
            $result = $this->client->count($params);
            return intval($result['count'] ?? 0);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return 0;
    }

    /**
     * 聚合sum查询
     * @param string $field
     * @return int
     */
    public function sum(string $field = '')
    {
        try {
            if(empty($this->options['query'])){
                throw new \Exception('query条件不能为空');
            }

            $body = [
                "query" => $this->options['query'],
                'size'  => 0,
                'aggs' => [
                    'sum' => [
                        'sum' => [
                            'field' => $field
                        ]
                    ]
                ]
            ];
            $params = [
                'index' => $this->options['index'],
                'body' => $body
            ];
            $this->options['query'] = [];
            $result = $this->client->search($params);
            return intval($result['aggregations']['sum']['value'] ?? 0);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return 0;
    }

    /**
     * 删除一条数据
     * @param int $id
     * @return array
     */
    public function delete(int $id = 0)
    {
        try {
            if($id) {
                $params = [
                    'index' => $this->options['index'],
                    'id' => $id
                ];
                return $result = $this->client->delete($params);
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 创建一条数据
     * @param array $data
     * @return array
     */
    public function create(array $data = [])
    {
        try {
            if(!empty($data)) {
                $id = $data['id']?? 0;
                if($id) {
                    $params = [
                        'index' => $this->options['index'],
                        'id' => $id,
                        'body' => $data
                    ];
                    return $result = $this->client->create($params);
                }
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 更新一条数据
     * @param array $data
     * @return array
     */
    public function update(array $data = [])
    {
        try {
            if(!empty($data)) {
                $id = $data['id']?? 0;
                if($id) {
                    $params = [
                        'index' => $this->options['index'],
                        'id' => $id,
                        'body' => $data
                    ];
                    return $result = $this->client->update($params);
                }
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 创建索引
     * @param array $body
     * @param string $index
     * @return array
     */
    public function createIndex(array $body = [])
    {
        try {
            $params = [
                'index' => $this->options['index'],
                'body' => $body
            ];
            return $result = $this->client->indices()->create($params);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 删除索引
     * @return array
     */
    public function deleteIndex()
    {
        try {
            $params = [
                'index' => $this->options['index']
            ];
            return $result = $this->client->indices()->delete($params);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 获取索引信息
     * @return array
     */
    public function getIndex()
    {
        try {
            $params = [
                'index' => $this->options['index']
            ];
            return $this->client->indices()->get($params);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 异常信息格式化
     * @param \Throwable $e
     */
    private function _print_exception_info(\Throwable $e, $log_level = 'error')
    {
        // TODO::拆分独立模块
        $infoStr = ' err_code:' . $e->getCode() . PHP_EOL
            . ' err_msg:' . $e->getMessage() . PHP_EOL
            . ' err_file:' . $e->getFile() . PHP_EOL
            . ' err_line:' . $e->getLine() . PHP_EOL
            . ' err_trace:' . PHP_EOL
            . $e->getTraceAsString();
        $this->logger->{$log_level}($infoStr);
    }

}