package redis

import "fmt"

const nodePipelineMaxInFlight = 64

type nodePipelineResult struct {
	replies []interface{}
	err     error
}

type nodePipelineRequest struct {
	send    func(*redisConn) error
	receive func(*redisConn) ([]interface{}, error)
	result  chan nodePipelineResult
}

// newNodePipelineRequest 构造一个节点级 pipeline 请求。
// send/receive 分别描述“如何在连接上发送”和“如何按原顺序读取本请求的回复”。
func newNodePipelineRequest(
	send func(*redisConn) error,
	receive func(*redisConn) ([]interface{}, error),
) *nodePipelineRequest {
	return &nodePipelineRequest{
		send:    send,
		receive: receive,
		result:  make(chan nodePipelineResult, 1),
	}
}

// Wait 等待节点级 pipeline actor 完成当前请求。
// 调用方只关心自己的结果，不直接接触底层连接和收发顺序。
func (r *nodePipelineRequest) Wait() ([]interface{}, error) {
	result, ok := <-r.result
	if !ok {
		return nil, fmt.Errorf("node pipeline result channel closed")
	}
	return result.replies, result.err
}

// complete 回填请求结果并关闭结果通道。
// 结果只会写入一次，因此这里可以直接 close 通知等待方结束。
func (r *nodePipelineRequest) complete(replies []interface{}, err error) {
	r.result <- nodePipelineResult{replies: replies, err: err}
	close(r.result)
}

type nodePipeline struct {
	node    *redisNode
	reqCh   chan *nodePipelineRequest
	closeCh chan struct{}
	doneCh  chan struct{}
}

// newNodePipeline 为单个 Redis 节点启动一个长期存活的有序 pipeline actor。
// 同一个节点上的所有请求都会经由它复用同一条连接，并按 FIFO 顺序发送和收包。
func newNodePipeline(node *redisNode) *nodePipeline {
	p := &nodePipeline{
		node:    node,
		reqCh:   make(chan *nodePipelineRequest, nodePipelineMaxInFlight),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	go p.run()
	return p
}

// Submit 将请求提交到节点级 pipeline。
// 提交成功只表示请求已经进入该节点的有序队列，不表示已经发包或收到回复。
func (p *nodePipeline) Submit(req *nodePipelineRequest) error {
	select {
	case <-p.closeCh:
		return fmt.Errorf("node pipeline closed: %s", p.node.address)
	case p.reqCh <- req:
		return nil
	}
}

// Close 停止节点级 pipeline，并等待后台 actor 完全退出。
// 这里要等待 doneCh，避免调用方在关闭后仍误以为底层 goroutine 还在工作。
func (p *nodePipeline) Close() {
	select {
	case <-p.closeCh:
	default:
		close(p.closeCh)
	}
	<-p.doneCh
}

// run 是节点级 ordered pipeline 的核心循环。
// 它在单个 goroutine 中同时负责三件事：
// 1. 复用同一条节点连接按顺序发送请求；
// 2. 按请求入队顺序读取回复；
// 3. 在连接出错时把当前请求和所有未收完回复的请求一起失败掉。
func (p *nodePipeline) run() {
	defer close(p.doneCh)

	var conn *redisConn
	pending := make([]*nodePipelineRequest, 0, nodePipelineMaxInFlight)

	// 连接级错误后必须彻底丢弃旧连接，避免后续请求继续复用半坏状态。
	closeConn := func() {
		if conn != nil {
			conn.shutdown()
			conn = nil
		}
	}
	// 同一条连接一旦发生协议/网络错误，后续 pending 请求的回复边界已不可信，
	// 因此要整体失败，而不是继续尝试逐个读出。
	failPending := func(err error) {
		for _, req := range pending {
			req.complete(nil, err)
		}
		pending = pending[:0]
	}
	// sendReq 只负责“按顺序发出去”，发送完成后把请求挂到 pending 队列尾部，
	// 等待后续按 FIFO 顺序收回复。
	sendReq := func(req *nodePipelineRequest) {
		if conn == nil || conn.isClosed() {
			var err error
			conn, err = p.node.getConn()
			if err != nil {
				req.complete(nil, err)
				return
			}
		}
		if err := req.send(conn); err != nil {
			req.complete(nil, err)
			closeConn()
			failPending(err)
			return
		}
		pending = append(pending, req)
	}
	// receiveHead 只读取队头请求的回复。
	// 这是整个 ordered pipeline 正确性的关键：回复消费顺序必须与发送顺序完全一致。
	receiveHead := func() {
		req := pending[0]
		pending = pending[1:]

		replies, err := req.receive(conn)
		if err != nil {
			req.complete(nil, err)
			closeConn()
			failPending(err)
			return
		}
		req.complete(replies, nil)
	}

	for {
		if len(pending) == 0 {
			// 没有 in-flight 请求时优先阻塞等待新请求或关闭信号，
			// 避免空转轮询。
			select {
			case <-p.closeCh:
				closeConn()
				return
			case req := <-p.reqCh:
				sendReq(req)
			}
			continue
		}

		if len(pending) >= nodePipelineMaxInFlight {
			// in-flight 达到上限时停止继续发送，先回收最早一批回复，
			// 既控制内存/背压，也避免单节点无限堆积未读回复。
			receiveHead()
			continue
		}

		select {
		case <-p.closeCh:
			closeConn()
			failPending(fmt.Errorf("node pipeline closed: %s", p.node.address))
			return
		case req := <-p.reqCh:
			// 连接可继续承载新请求时，优先把后续请求也发出去，
			// 这样才能体现 pipeline“先发多笔、后续逐笔收包”的收益。
			sendReq(req)
		default:
			// 当前没有新请求可发时，主动推进最早请求的回复读取。
			receiveHead()
		}
	}
}
