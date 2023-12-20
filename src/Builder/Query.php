<?php

namespace Es\Builder;

use Es\Builder\Connection;
use Es\Es;

class Query
{

    /**
     * 当前es连接对象
     * @var Connection
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

    public function __construct(Connection $client = null)
    {
        if (is_null($client)) {
            $this->client = Es::connect();
        } else {
            $this->client = $client;
        }

        self::$index = '';
        self::$order = [];
        self::$aggs = [];
        self::$group = [];
        self::$query = [];
        self::$from = 0;
        self::$size = 0;
        self::$highlight = [];
    }

    public function index($index = '')
    {
        self::$index = $index;
        return $this;
    }

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
        } catch (\Exception $e) {
        }
        return false;
    }

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
        } catch (\Exception $e) {
        }
        return [];
    }


    public function index_old(array $data = [], string $index = '')
    {
        try {
            $index && self::$index = $index;
            if(!empty($data)) {
                $id = $data['id'] ?? 0;
                $params = [
                    'id' => $id,
                    'index' => self::$index,
                    'body' => $data
                ];
                return $result = $this->client->index($params);
            }
        } catch (\Exception $e) {
        }
        return [];
    }

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
        } catch (\Exception $e) {
        }
        return [];
    }

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
        } catch (\Exception $e) {
        }
        return [];
    }

    /**
     * 多条sql查询
     * @param string $index
     * @return array
     * @author 李静
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
        }
        return [];
    }

    /**
     * 分页查询
     * @param $page_size
     * @param $page
     * @param $index
     * @return array
     * @author 李静
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

            $data['data'] = $list;

            return $data;
        } catch (\Throwable $e) {
        }
    }

    /**
     * 单条sql查询
     * @param string $index
     * @return array
     * @author 李静
     */
    public function find(string $index = '')
    {
        try {

            self::$from = 0;
            self::$size = 1;

            $result = $this->_search($index);

            return $result['hits']['hits'][0]['_source'] ?? [];
        } catch (\Throwable $e) {
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
     * @author 李静
     */
    public function whereOr($map = [])
    {
        $temp_query = [];
        foreach ($map as $item) {
            if(count($item) != 3){
                Throw new \Exception("sql格式错误");
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
                        Throw new \Exception("sql格式between错误");
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
                    Throw new \Exception("sql格式错误");
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
     * 查询sql参数格式化
     * @param $map
     * @return $this
     * @author 李静
     */
    public function where($map = [])
    {
        foreach ($map as $item) {
            if(count($item) != 3){
                Throw new \Exception("sql格式错误");
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
                        Throw new \Exception("sql格式between错误");
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
                    Throw new \Exception("sql格式错误");
            }
        }

        return $this;
    }

    /**
     * 查询sql参数格式化
     * @param $fields
     * @return $this
     * @author 李静
     */
    public function whereNull($fields = [])
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
     * 查询sql参数格式化
     * @param $fields
     * @return $this
     * @author 李静
     */
    public function whereNotNull($fields = [])
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
     * 查询封装
     * @param string $index
     * @return array|callable
     * @author 李静
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

        Log::debug('最终请求参数==========', json_encode($params, JSON_UNESCAPED_UNICODE));
        $result = $this->client->search($params);
//        Log::debug('最终请求数据==========', json_encode($result, JSON_UNESCAPED_UNICODE));
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
     * 统计查询
     * @param string $index
     * @return int
     * @author 李静
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
        }
        return 0;
    }

    /**
     * 统计查询
     * @param string $field
     * @param string $index
     * @return int
     * @author 李静
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
        }
        return 0;
    }

    /**
     * 临时封装count(*) group 查询方法，其他人请勿使用
     * @param $in_map
     * @param $field
     * @return array
     * @author 李静
     */
    public function groupCount($in_map = [], $field = '')
    {
        /*$param = [
            'size'  => 0,
            "query" => ["bool" => ["must"=>[["terms"=>['tender_principal'=>$principal_ids]],]]],"aggs"=>["tender_principals"=>["terms"=>["field"=>"tender_principal",],"aggs"=>["num"=>["value_count"=>["field"=>"id"]],]]]
        ];*/
        $field = $field?: 'id';
        $param = [
            'size'  => 0,
            "query" => ["bool" => ["must"=>[["terms"=>$in_map],]]],"aggs"=>["tender_principal"=>["terms"=>["field"=>$field,],"aggs"=>["group_get_num"=>["value_count"=>["field"=>$field]],]]]
        ];
        $data = $this->search($param);
        $data = $data['aggregations']['tender_principal']['buckets'] ?? [];
        $data = array_column($data, 'group_get_num', 'key');
        $data = array_combine(array_keys($data), array_column($data, 'value'));

        return $data;
    }

    public function order($order = [])
    {
        $order_list = [];
//        $order_list[]['_score']['order'] = 'desc';
        foreach ($order as $key => $value) {
            $order_list[][$key]['order'] = $value;
        }

        self::$order = $order_list;
        return $this;
    }

    /**
     * 查询条数
     * @param $num
     * @return $this
     * @author 李静
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
     * @author 李静
     */
    public function limit($num = 0)
    {
        self::$size = $num;

        return $this;
    }

    /**
     * 查询是否高亮
     * @param $fields
     * @return $this
     * @author 李静
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
        } catch (\Exception $e) {
        }
        return [];
    }

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
        } catch (\Exception $e) {
        }
        return [];
    }

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
        } catch (\Exception $e) {
        }
        return [];
    }

    public function createIndex(array $body = [], string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index,
                'body' => $body
            ];
            return $result = $this->client->indices()->create($params);
        } catch (\Exception $e) {
        }
        return [];
    }

    public function deleteIndex(string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index
            ];
            return $result = $this->client->indices()->delete($params);
        } catch (\Exception $e) {
        }
        return [];
    }

    public function getIndex(string $index = '')
    {
        try {
            $index && self::$index = $index;
            $params = [
                'index' => self::$index
            ];
            return $result = $this->client->indices()->get($params);
        } catch (\Exception $e) {
        }
        return [];
    }

}