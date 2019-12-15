package stream

import (
	"dag-stream/logger"
	"errors"
	"time"
)

var emitter_map = make(map[string]Emitter)
var handler_map = make(map[string]Handler)

func RegistEmitter(name string, e Emitter) {
	emitter_map[name] = e
}
func RegistHandler(name string, h Handler) {
	handler_map[name] = h
}

func GetEmitter(name string) (emitter Emitter, exsit bool) {
	a, b := emitter_map[name]
	return a, b
}

func GetHandler(name string) (handler Handler, exsit bool) {
	a, b := handler_map[name]
	return a, b
}

type Emitter interface {
	Init(func(interface{})) error
	Close() error
}

type Handler interface {
	Init(interface{}) error
	Handle(interface{}, func(interface{}))
	Close() error
}

//type Emitter struct {
//	Init  func(func(interface{})) error
//	Close func() error
//}
//
//type Handler struct {
//	Init   func(interface{}) error
//	Handle func(interface{}, func(interface{}))
//	Close  func() error
//}

type Stream struct {
	inited   bool
	running  bool
	emitter  Emitter
	handlers []Handler
}

func (s *Stream) linkHandle(index int, data interface{}) {
	if len(s.handlers) > index {
		h := s.handlers[index]
		h.Handle(data, func(ndata interface{}) {
			index++
			s.linkHandle(index, ndata)
		})
	}
}

func Build(flow []string) (s *Stream, err error) {
	if len(flow) == 0 {
		return nil, errors.New("未定义流程处理环节")
	}
	//check
	var emitter Emitter
	var handlers = make([]Handler, 0)

	for i, name := range flow {
		if i == 0 {
			e, ok := GetEmitter(name)
			if !ok {
				err := errors.New("未注册的Emitter:" + name)
				return nil, err
			}
			emitter = e
		} else {
			h, ok := GetHandler(name)
			if !ok {
				err := errors.New("未注册的Handler:" + name)
				return nil, err
			}
			handlers = append(handlers, h)
		}
	}

	myStream := New(emitter)
	for i := 0; i < len(handlers); i++ {
		myStream.Pipe(handlers[i])
	}
	return myStream, nil
}

func New(emitter Emitter) *Stream {
	s := &Stream{}
	s.handlers = make([]Handler, 0)
	s.emitter = emitter
	return s
}

func (s *Stream) Pipe(h Handler) *Stream {
	s.handlers = append(s.handlers, h)
	return s
}

func (s *Stream) Init() error {
	var err error
	s.inited = false
	for _, h := range s.handlers {
		err = h.Init(nil)
		if err != nil {
			break
		}
	}
	if err != nil {
		logger.LOG_ERROR("处理流程初始化异常,启动失败！：", err)
	} else {
		s.inited = true
	}
	return err
}

func (s *Stream) Run() {
	if !s.inited {
		return
	}
	err := s.emitter.Init(func(data interface{}) {
		start := time.Now()
		s.linkHandle(0, data)
		logger.LOG_INFO("单轮耗时：%v", time.Since(start))
	})

	if err != nil {
		logger.LOG_ERROR("采集器初始化异常,启动失败！：", err)
	} else {
		s.running = true
	}
}

func (s *Stream) Close() {
	var err error
	if s.emitter != nil {
		err = s.emitter.Close()
	}
	if err != nil {
		logger.LOG_WARN("关闭stream异常：", err)
	}
	for _, h := range s.handlers {
		err = h.Close()
		if err != nil {
			logger.LOG_WARN("关闭stream异常：", err)
		}
	}
}

//func EmiterFromPlugin(path string) Emitter {
//	fmt.Println(path)
//	p, _ := plugin.Open(path)
//	init, _ := p.Lookup("Init")
//	closem, _ := p.Lookup("Close")
//	return struct{
//		Init func(func(interface{})) error
//		Close func() error
//	}{
//		Init: init.(func(func(interface{})) error),
//		Close: closem.(func() error),
//	}
//}
//
//func HandlerFromPlugin(path string) Handler {
//	fmt.Println(path)
//	p, _ := plugin.Open(path)
//	init, _ := p.Lookup("Init")
//	handle, _ := p.Lookup("Handle")
//	return Handler{
//		Init:   init.(func(interface{}) error),
//		Handle: handle.(func(interface{}, func(interface{}))),
//	}
//}

type EmitterWrapper struct {
	InitFunc  func(func(interface{})) error
	CloseFunc func() error
}

func (ew *EmitterWrapper) Init(emit func(interface{})) error {
	return ew.InitFunc(emit)
}
func (ew *EmitterWrapper) Close() error {
	if ew.InitFunc == nil {
		return nil
	}
	return ew.CloseFunc()
}

type HandlerWrapper struct {
	InitFunc   func(interface{}) error
	HandleFunc func(interface{}, func(interface{}))
	CloseFunc  func() error
}

func (ew *HandlerWrapper) Init(config interface{}) error {
	if ew.InitFunc == nil {
		return nil
	}
	return ew.InitFunc(config)
}
func (ew *HandlerWrapper) Handle(data interface{}, next func(interface{})) {
	ew.HandleFunc(data, next)
}
func (ew *HandlerWrapper) Close() error {
	if ew.CloseFunc == nil {
		return nil
	}
	return ew.CloseFunc()
}
