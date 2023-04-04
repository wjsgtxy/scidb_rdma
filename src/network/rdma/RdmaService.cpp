// The header file for the implementation details in this file
#include "RdmaService.h"
#include "RdmaProperties.h"
#include "RdmaCommManager.h"
// SciDB modules
#include <system/Config.h>
#include <util/Job.h>
#include <util/JobQueue.h>
#include <util/ThreadPool.h>
// third-party libraries
#include <log4cxx/logger.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.rdma.RdmaService"));
}

namespace scidb {
//    namespace rdma {

const std::string serviceName("RdmaService");
const std::string serviceJobName("RdmaServiceJob");

/*
 * 具体实现，对外屏蔽细节
 */
struct RdmaService::Impl
{
    Impl();
    ~Impl() noexcept;

    void start();
    void stop();

    // 一个具体的任务，会push到下面的JobQueue里面去，在线程池中执行
    class RdmaServiceJob : public scidb::Job
    {
    public:
        RdmaServiceJob();
        virtual ~RdmaServiceJob() noexcept = default;

        void run() override; // 这个里面启动 rdma conn manager
        void stop(); // 调用rdma conn mgr的stop

    private:
        // std::shared_ptr<RdmaCommManager> _manager{nullptr}; // dz 注意，这里的manager是一个 share ptr
        RdmaCommManager* _manager;
    };

    std::shared_ptr<JobQueue> _queue{nullptr};
    std::shared_ptr<ThreadPool> _threadPool{nullptr};
    std::shared_ptr<RdmaServiceJob> _myJob{nullptr};
    bool _running{false};
};

RdmaService::RdmaService() : _impl(std::make_unique<RdmaService::Impl>())
{}

RdmaService::~RdmaService() noexcept = default;

void RdmaService::start()
{
    _impl->start();
}

void RdmaService::stop()
{
    _impl->stop();
}

// service实现impl的构造函数
RdmaService::Impl::Impl()
        : _queue(std::shared_ptr<JobQueue>(new JobQueue(serviceName)))
        , _threadPool(std::shared_ptr<ThreadPool>(new ThreadPool(1, _queue, serviceName))) // dz 创建只有一个线程的线程池
{}

RdmaService::Impl::~Impl() noexcept
{
    try {
        // 停止线程池
        LOG4CXX_TRACE(logger, "Shutting down RdmaService threadPool");
        _threadPool->stop();
    } catch (std::exception const& e) {
        // threadPool can throw (deep in Semaphore::release).
        LOG4CXX_WARN(logger, "RdmaService failed to stop thread pool during destruction. (" << e.what() << ")");
    }
}

void RdmaService::Impl::start()
{
    ASSERT_EXCEPTION(!_threadPool->isStarted(), "Rdma service started multiple times.");

    LOG4CXX_INFO(logger, "Starting RdmaService");
    _threadPool->start();
    // dz push一个RdmaServiceJob进去，thread应该会调用job的run方法。
    _myJob = std::make_shared<RdmaServiceJob>();
    _queue->pushJob(_myJob);
    _running = true;
}

void RdmaService::Impl::stop()
{
    LOG4CXX_INFO(logger, "Stopping RdmaService");
    if (_myJob) {
        _myJob->stop();
        _myJob.reset();
    }
    _running = false;
    // 资源释放，都是智能指针管理，自动释放资源
}

RdmaService::Impl::RdmaServiceJob::RdmaServiceJob()
        : Job(std::shared_ptr<Query>(), serviceJobName) // 这里用了一个空的query

{
    _manager = RdmaCommManager::getInstance();
}

void RdmaService::Impl::RdmaServiceJob::run()
{
    // 在这个里面启动rdma comm manager
//    ASSERT_EXCEPTION(!_manager, "RdmaCommService Job: error on start; already running"); // 现在只有一个了

    int start_count = 0;
    bool stopped = false;
    while (!stopped && start_count++ < 1) { // 先改成1了，只允许启动一个
        LOG4CXX_INFO(logger, "Starting " << name() << ": start_count:" << start_count);
//        SCIDB_ASSERT(!_manager); // 现在只有一个了，不需要这个了
        RdmaProperties props;
        props.setPort(Config::getInstance()->getOption<int>(CONFIG_CCM_PORT))
                .setTLS(Config::getInstance()->getOption<int>(CONFIG_CCM_TLS))
                .setSessionTimeOut(Config::getInstance()->getOption<int>(CONFIG_CCM_SESSION_TIME_OUT))
                .setReadTimeOut(Config::getInstance()->getOption<int>(CONFIG_CCM_READ_TIME_OUT));
        try {
            // dz 创建一个rdma连接管理器
//            _manager = std::make_shared<RdmaCommManager>(props);
//            _manager->run(); // 这个里面 run了之后，就要一直循环才可以了
//            _manager = RdmaCommManager::getInstance(); // 初始化的时候已经创建了
              _manager->run();
        } catch (boost::system::system_error& e) {
            // 监听端口已经使用了
            if (e.code().value() == boost::asio::error::address_in_use) {
                // This instance is NOT the winner to bind to the Rdma port.
                LOG4CXX_DEBUG(logger, "RdmaCommManager could not start... " << e.what());
                // 直接停止
                stopped = true;
            } else {
                LOG4CXX_WARN(logger, "Failed to start RdmaCommManager: " << e.what());
            }
        } catch (std::exception const& e) {
            LOG4CXX_ERROR(logger, "Failed to start RdmaCommManager: " << e.what());
        }
        // 不需要了, RdmaServiceJob的析构里面会调用stop释放
//        _manager.reset(); // _manager是share ptr，reset方法会释放管理的对象，会调用mgr的析构函数
    }
}

void RdmaService::Impl::RdmaServiceJob::stop()
{
    if (_manager) {
        _manager->stop();
    }
}

//}
}  // namespace scidb::ccm
