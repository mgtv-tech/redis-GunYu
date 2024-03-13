package replica

// import (
// 	"net"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/reflection"
// )

// func NewServer(listen string) error {
// 	ServerOptions := []grpc.ServerOption{}
// 	svr := grpc.NewServer(ServerOptions...)
// 	//register(svr)
// 	//svr.RegisterService()
// 	reflection.Register(svr)
// 	listener, err := net.Listen("tcp", listen)
// 	if err != nil {
// 		return err
// 	}

// 	return svr.Serve(listener)
// }
