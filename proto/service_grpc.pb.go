// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.4.0
// - protoc             v5.28.0--rc1
// source: service.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.62.0 or later.
const _ = grpc.SupportPackageIsVersion8

const (
	KafkaService_Produce_FullMethodName     = "/pb.KafkaService/Produce"
	KafkaService_Consume_FullMethodName     = "/pb.KafkaService/Consume"
	KafkaService_CreateTopic_FullMethodName = "/pb.KafkaService/CreateTopic"
)

// KafkaServiceClient is the client API for KafkaService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KafkaServiceClient interface {
	Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error)
	Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error)
	CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error)
}

type kafkaServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewKafkaServiceClient(cc grpc.ClientConnInterface) KafkaServiceClient {
	return &kafkaServiceClient{cc}
}

func (c *kafkaServiceClient) Produce(ctx context.Context, in *ProduceRequest, opts ...grpc.CallOption) (*ProduceResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ProduceResponse)
	err := c.cc.Invoke(ctx, KafkaService_Produce_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kafkaServiceClient) Consume(ctx context.Context, in *ConsumeRequest, opts ...grpc.CallOption) (*ConsumeResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ConsumeResponse)
	err := c.cc.Invoke(ctx, KafkaService_Consume_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kafkaServiceClient) CreateTopic(ctx context.Context, in *CreateTopicRequest, opts ...grpc.CallOption) (*CreateTopicResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateTopicResponse)
	err := c.cc.Invoke(ctx, KafkaService_CreateTopic_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KafkaServiceServer is the server API for KafkaService service.
// All implementations must embed UnimplementedKafkaServiceServer
// for forward compatibility
type KafkaServiceServer interface {
	Produce(context.Context, *ProduceRequest) (*ProduceResponse, error)
	Consume(context.Context, *ConsumeRequest) (*ConsumeResponse, error)
	CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error)
	mustEmbedUnimplementedKafkaServiceServer()
}

// UnimplementedKafkaServiceServer must be embedded to have forward compatible implementations.
type UnimplementedKafkaServiceServer struct {
}

func (UnimplementedKafkaServiceServer) Produce(context.Context, *ProduceRequest) (*ProduceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Produce not implemented")
}
func (UnimplementedKafkaServiceServer) Consume(context.Context, *ConsumeRequest) (*ConsumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Consume not implemented")
}
func (UnimplementedKafkaServiceServer) CreateTopic(context.Context, *CreateTopicRequest) (*CreateTopicResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTopic not implemented")
}
func (UnimplementedKafkaServiceServer) mustEmbedUnimplementedKafkaServiceServer() {}

// UnsafeKafkaServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KafkaServiceServer will
// result in compilation errors.
type UnsafeKafkaServiceServer interface {
	mustEmbedUnimplementedKafkaServiceServer()
}

func RegisterKafkaServiceServer(s grpc.ServiceRegistrar, srv KafkaServiceServer) {
	s.RegisterService(&KafkaService_ServiceDesc, srv)
}

func _KafkaService_Produce_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProduceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KafkaServiceServer).Produce(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KafkaService_Produce_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KafkaServiceServer).Produce(ctx, req.(*ProduceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KafkaService_Consume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConsumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KafkaServiceServer).Consume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KafkaService_Consume_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KafkaServiceServer).Consume(ctx, req.(*ConsumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KafkaService_CreateTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KafkaServiceServer).CreateTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: KafkaService_CreateTopic_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KafkaServiceServer).CreateTopic(ctx, req.(*CreateTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// KafkaService_ServiceDesc is the grpc.ServiceDesc for KafkaService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KafkaService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.KafkaService",
	HandlerType: (*KafkaServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Produce",
			Handler:    _KafkaService_Produce_Handler,
		},
		{
			MethodName: "Consume",
			Handler:    _KafkaService_Consume_Handler,
		},
		{
			MethodName: "CreateTopic",
			Handler:    _KafkaService_CreateTopic_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service.proto",
}
