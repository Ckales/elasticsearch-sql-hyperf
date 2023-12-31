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

    private static $index;
    private static $order;
    private static $aggs;
    private static $group;
    private static $query;
    private static $size;
    private static $from;
    private static $highlight;
    private static $_source;
    private static $debug = false;
    protected LoggerInterface $logger;

    public function __construct(Client $client = null, $config = [])
    {
        if (is_null($client)) {
            $this->client = Es::connect();
        } else {
            $this->client = $client;
        }

        $this->logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('elasticsearch-sql-hyperf', $config['log_config'] ?? 'default');

        self::$index = '';
        self::$order = [];
        self::$aggs = [];
        self::$group = [];
        self::$query = [];
        self::$from = 0;
        self::$size = 0;
        self::$highlight = [];
        self::$_source = [];
        self::$debug = $config['debug'] ?? false;
    }

    /**
     * 设置索引
     * @param string $index
     * @return $this
     */
    public function index($index = '')
    {
        self::$index = $index;
        return $this;
    }

    /**
     * 判断文档是否存在
     * @param int $id 文档id
     * @param string $index 索引
     * @return bool
     */
    public function exists(int $id = 0, string $index = '')
    {
        try {
            $index && self::$index = $index;
            if($id) {
                $params = [
                    'id' => $id,
                    'index' => self::$index,
                ];
                return $result = $this->client->exists($params);
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return false;
    }

    /**
     * 获取一个文档
     * @param int $id 文档id
     * @param string $index 索引
     * @return array
     */
    public function get(int $id = 0, string $index = '')
    {
        try {
            $index && self::$index = $index;
            if($id) {
                $params = [
                    'id' => $id,
                    'index' => self::$index,
                ];
                return $result = $this->client->get($params);
            }
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
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
            $index && self::$index = $index;
            if(!empty($list)) {
                $params = ['body'=>[]];
                foreach ($list as $data) {
                    $id = $data['id'] ?? 0;
                    $params['body'][] = [
                        'index' => [     //注意这里的动作为index, 表示库中有就更新，没有就创建
                            '_index' => self::$index,   //注意这里的字段前缀是_
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
            $index && self::$index = $index;
            $params = [
                'index' => self::$index,
                'body' => $query
            ];
            $result = $this->client->search($params);
            return $result;
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 多条数据查询
     * @param string $index
     * @return array
     * @author ChingLi
     */
    public function select(string $index = '')
    {
        try {

            $result = $this->_search($index);

            if(!empty($result['hits']['hits'])){
                $list = array_column($result['hits']['hits'], '_source');
            }else{
                $list = [];
            }
            return $list;
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 分页查询
     * @param $page_size
     * @param $page
     * @param $index
     * @return array
     * @author ChingLi
     */
    public function paginate($page_size = 10, $page = 1)
    {
        try {
            $page = max($page, 1);
            self::$from = ($page - 1) * $page_size;
            self::$size = $page_size;

            $result = $this->_search(self::$index);

            $total = $result['hits']['total']['value'] ?? 0;

            if(!empty($result['hits']['hits'])){
                $list = array_column($result['hits']['hits'], '_source');
            }else{
                $list = [];
            }

            $data = $this->_imitate_page($total,$page_size, $page);
            $data['data'] = $list;

        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
            $data = $this->_imitate_page(0,$page_size, $page);
        }
        return $data;
    }



    /**
     * 模拟分页
     * @param $total
     * @param $limit
     * @param $current_page
     * @return array
     * @author ChingLi
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
     * 单条数据查询
     * @param string $index
     * @return array
     * @author ChingLi
     */
    public function find(string $index = '')
    {
        try {

            self::$from = 0;
            self::$size = 1;

            $result = $this->_search($index);

            return $result['hits']['hits'][0]['_source'] ?? [];
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * where or查询
     * 注意！！！当多个不同的whereOR条件查询时，请使用多个whereOr进行拼接！！！
     * ！！！例如：MYSQL中 where (id = '1' or id = '2') and (name = '3' or title = '4')！！！
     * ！！！请使用->whereOr([['id', '=', '1'], ['id', '=', '2']])->whereOr([['name', '=', '3'], ['title', '=', '4']])进行拼接！！！
     * @param string $map
     * @return $this
     * @author ChingLi
     */
    public function whereOr($map = [])
    {
        $temp_query = [];
        foreach ($map as $item) {
            if(count($item) != 3){
                Throw new \Exception("格式错误");
            }
            switch ($item[1]) {
                case '=':
                    $temp_query[] = [
                        "term" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case '>':
                    $temp_query[] = [
                        "range" => [
                            $item[0] => [
                                "gt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '>=':
                    $temp_query[] = [
                        "range" => [
                            $item[0] => [
                                "gte" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<':
                    $temp_query[] = [
                        "range" => [
                            $item[0] => [
                                "lt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<=':
                    $temp_query[] = [
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
                    $temp_query[] = [
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
                    $temp_query[] = [
                        "terms" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case 'like':
                    $temp_query[] = [
                        "match_phrase" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;
                case 'vague_like':
                    $temp_query[] = [
                        "match" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;

                default:
                    Throw new \Exception("格式错误");
            }
        }
        if(!empty($temp_query)){
            self::$query['bool']['must'][] = [
                "bool" => [
                    "minimum_should_match" => 1,
                    "should" => $temp_query
                ]
            ];
        }
        return $this;
    }

    /**
     * 查询参数格式化
     * @param $map
     * @return $this
     * @author ChingLi
     */
    public function where($map = [])
    {
        foreach ($map as $item) {
            if(count($item) != 3){
                Throw new \Exception("格式错误");
            }
            switch ($item[1]) {
                case '=':
                    self::$query['bool']['must'][] = [
                        "term" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case '<>':
                case '!=':
                    self::$query['bool']['must_not'][] = [
                        "term" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case '>':
                    self::$query['bool']['must'][] = [
                        "range" => [
                            $item[0] => [
                                "gt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '>=':
                    self::$query['bool']['must'][] = [
                        "range" => [
                            $item[0] => [
                                "gte" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<':
                    self::$query['bool']['must'][] = [
                        "range" => [
                            $item[0] => [
                                "lt" => $item[2],
                            ]
                        ]
                    ];
                    break;
                case '<=':
                    self::$query['bool']['must'][] = [
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
                    self::$query['bool']['must'][] = [
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
                    self::$query['bool']['must'][] = [
                        "terms" => [
                            $item[0] => $item[2]
                        ]
                    ];
                    break;
                case 'like':
                    self::$query['bool']['must'][] = [
                        "match_phrase" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;
                case 'vague_like':
                    self::$query['bool']['must'][] = [
                        "match" => [
                            $item[0] => str_replace('%', '*', $item[2])
                        ]
                    ];
                    break;
                default:
                    Throw new \Exception("格式错误");
            }
        }

        return $this;
    }

    /**
     * 查询参数格式化
     * @param $fields
     * @return $this
     * @author ChingLi
     */
    public function whereNotNull($fields = [])
    {
        foreach ($fields as $item) {
            self::$query['bool']['must'][] = [
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
     * @author ChingLi
     */
    public function whereNull($fields = [])
    {
        foreach ($fields as $item) {
            self::$query['bool']['must_not'][] = [
                "exists" => [
                    'field' => $item
                ]
            ];
        }

        return $this;
    }

    /**
     * 查询字段
     * @param $fileds
     * @return $this
     */
    public function field($fileds = [])
    {
        if(!empty($fileds) && is_array($fileds)){
            self::$_source = $fileds;
        }

        return $this;
    }

    /**
     * 查询封装
     * @param string $index
     * @return array|callable
     * @author ChingLi
     */
    private function _search(string $index = '')
    {
        $index && self::$index = $index;

        $body = [];
        if(!empty(self::$query)){
            $body["query"] = self::$query;
        }

        if(!empty(self::$group)){
            $body['aggs'] = ['group_by' => self::$group];
        }

        if(!empty(self::$aggs)){
            if(!empty($body['aggs'])){
                $body['aggs']['group_by']['aggs'] = self::$aggs;
            }else{
                $body['aggs'] = self::$aggs;
            }
        }

        if(!empty(self::$order)){
            $body['sort'] = self::$order;
        }

        if(self::$from){
            $body['from'] = self::$from;
        }

        if(self::$size){
            $body['size'] = self::$size;
        }

        if(!empty(self::$highlight)){
            $body['highlight'] = self::$highlight;
        }

        $params = [
            'index' => self::$index,
            'body' => $body
        ];

        if(!empty(self::$_source)){
            $params['_source'] = self::$_source;
        }

        if(self::$debug){
            $this->logger->debug('最终请求参数==========' . json_encode($params, JSON_UNESCAPED_UNICODE));
        }
        $result = $this->client->search($params);

        if(self::$debug){
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

        self::$query = [];
        self::$order = [];
        self::$aggs = [];
        self::$from = 0;
        self::$size = 0;
        self::$highlight = [];

        return $result;
    }

    /**
     * 聚合count查询
     * @param string $index
     * @return int
     * @author ChingLi
     */
    public function count(string $index = '')
    {
        try {
            $index && self::$index = $index;

            $body = [
                "query" => self::$query,
            ];
            $params = [
                'index' => self::$index,
                'body' => $body
            ];
            self::$query = [];
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
     * @param string $index
     * @return int
     * @author ChingLi
     */
    public function sum(string $field = '', string $index = '')
    {
        try {
            $index && self::$index = $index;

            $body = [
                "query" => self::$query,
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
                'index' => self::$index,
                'body' => $body
            ];
            self::$query = [];
            $result = $this->client->search($params);
            return intval($result['aggregations']['sum']['value'] ?? 0);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return 0;
    }

    /**
     * 对count(*) group by 进行封装
     * 对应mysql select count()，sum() from table group by {$group_by_field}
     * @param $aggs array 聚合查询，格式：
     * [
     *      ['字段', '聚合查询类型', '别名']
     *      ['num', 'sum', 'total_num']
     *      ['id', 'count', 'total_count']
     * ]
     * @param $group_by_field string group by字段
     * 支持链式order方法、imit方法
     * @return array
     * @throws \Exception
     * @author 李静
     */
    public function groupAggs($aggs = [], $group_by_field = '')
    {
        if(empty($aggs) || empty($group_by_field)){
            return [];
        }

        $group_key = 'group_num_key';

        $param = [
            'size'  => 0,
            "query" => self::$query,
        ];

        $aggs_query = [
            $group_key=>[
                "terms"=>["field"=>$group_by_field,]
            ]
        ];

        // 排序方式
        if(!empty(self::$order)){
            foreach (self::$order as $key => $item) {
                $temp_key = key($item);
                $aggs_query[$group_key]['terms']['order'][$temp_key] = $item[$temp_key]['order'];
            }
        }

        // 筛选数量
        if(!empty(self::$size)){
            $aggs_query[$group_key]['terms']['size'] = self::$size;
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

            $aggs_query[$group_key]['aggs'][$item[2]] = [$item[1]=>["field"=>$item[0]]];
        }

        $param['aggs'] = $aggs_query;

        // 进行数据查询
        $res = $this->search($param);
        $res = $res['aggregations'][$group_key]['buckets'] ?? [];

        $data = [];
        foreach ($res as $key => $value) {
            $tmep_data = [
                $group_by_field => $value['key'],
            ];

            foreach ($aggs as $item) {
                $tmep_data[$item[2]] = $value[$item[2]]['value'];
            }

            $data[] = $tmep_data;
        }

        return $data;
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

        self::$order = $order_list;
        return $this;
    }

    /**
     * 起始位置
     * @param $num
     * @return $this
     * @author ChingLi
     */
    public function offset($num = 0)
    {
        self::$from = $num;

        return $this;
    }

    /**
     * 查询条数
     * @param $num
     * @return $this
     * @author ChingLi
     */
    public function limit($num = 0)
    {
        self::$size = $num;

        return $this;
    }

    /**
     * 字段高亮
     * 格式['title', 'name']
     * @param $fields
     * @return $this
     * @author ChingLi
     */
    public function highlight($fields = [])
    {
        if(!empty($fields)){
            $highlight_fields = [];
            foreach ($fields as $item) {
                $highlight_fields[$item] = new \stdClass();
            }

            $highlight = [
                "order"  => "score",
                "boundary_scanner" => "sentence",
                'number_of_fragments'   => 0,
                "pre_tags" => ['<span style="color: red;">'],
                "post_tags" => ['</span>'],
                "fields" => $highlight_fields,
            ];

            self::$highlight = $highlight;
        }

        return $this;
    }

    /**
     * 删除一条数据
     * @param int $id
     * @param string $index
     * @return array
     */
    public function delete(int $id = 0, string $index = '')
    {
        try {
            $index && self::$index = $index;
            if($id) {
                $params = [
                    'index' => self::$index,
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
     * @param string $index
     * @return array
     * @author 李静
     */
    public function create(array $data = [], string $index = '')
    {
        try {
            $index && self::$index = $index;
            if(!empty($data)) {
                $id = $data['id']?? 0;
                if($id) {
                    $params = [
                        'index' => self::$index,
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
     * @param string $index
     * @return array
     */
    public function update(array $data = [], string $index = '')
    {
        try {
            $index && self::$index = $index;
            if(!empty($data)) {
                $id = $data['id']?? 0;
                if($id) {
                    $params = [
                        'index' => self::$index,
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
    public function createIndex(array $body = [], string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index,
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
     * @param string $index
     * @return array
     */
    public function deleteIndex(string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index
            ];
            return $result = $this->client->indices()->delete($params);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 获取索引信息
     * @param string $index
     * @return array
     * @author 李静
     */
    public function getIndex(string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index
            ];
            return $result = $this->client->indices()->get($params);
        } catch (\Throwable $e) {
            $this->_print_exception_info($e);
        }
        return [];
    }

    /**
     * 异常信息格式化
     * @param \Throwable $e
     * @return string
     * @author ChingLi
     */
    private function _print_exception_info(\Throwable $e, $log_level = 'error')
    {
        $infoStr = ' err_code:' . $e->getCode() . PHP_EOL
            . ' err_msg:' . $e->getMessage() . PHP_EOL
            . ' err_file:' . $e->getFile() . PHP_EOL
            . ' err_line:' . $e->getLine() . PHP_EOL
            . ' err_trace:' . PHP_EOL
            . $e->getTraceAsString();
        $this->logger->{$log_level}($infoStr);
    }

}