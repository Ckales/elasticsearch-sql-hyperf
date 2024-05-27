<?php

namespace Es\Builder;

use Psr\Log\LoggerInterface;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Context\ApplicationContext;

class Log
{

    public LoggerInterface $logger;

    private $debug = false;

    public function __construct($config = [])
    {

        $this->logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('elasticsearch-sql-hyperf', $config['log_config'] ?? 'default');

        $this->debug = $config['debug'] ?? false;
    }

    /**
     * 日志记录
     * @param $log
     * @param $log_level string 日志级别
     * @return bool
     */
    public function write($log = '', $log_level = '')
    {
        // 设置默认日志等级
        if(empty($log_level)){
            $log_level = 'debug';
        }

        if($this->debug){
            $this->logger->{$log_level}($log);
        }

        return true;
    }


    /**
     * 异常信息格式化
     * @param \Throwable $e
     * @param string $log_level 日志级别
     * @return bool
     */
    public function printExceptionInfo(\Throwable $e, $log_level = 'error')
    {
        $infoStr = ' err_code:' . $e->getCode() . PHP_EOL
            . ' err_msg:' . $e->getMessage() . PHP_EOL
            . ' err_file:' . $e->getFile() . PHP_EOL
            . ' err_line:' . $e->getLine() . PHP_EOL
            . ' err_trace:' . PHP_EOL
            . $e->getTraceAsString();
        $this->logger->{$log_level}($infoStr);

        return true;
    }
}