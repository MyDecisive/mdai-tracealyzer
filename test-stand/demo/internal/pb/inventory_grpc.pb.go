// Code generated for the demo gRPC contract. DO NOT EDIT.

package pb

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const (
	InventoryService_CheckAvailability_FullMethodName  = "/InventoryService/CheckAvailability"
	InventoryService_ReserveItems_FullMethodName       = "/InventoryService/ReserveItems"
	InventoryService_ReleaseReservation_FullMethodName = "/InventoryService/ReleaseReservation"
)

type InventoryServiceClient interface {
	CheckAvailability(ctx context.Context, in *InventoryRequest, opts ...grpc.CallOption) (*InventoryReply, error)
	ReserveItems(ctx context.Context, in *InventoryRequest, opts ...grpc.CallOption) (*InventoryReply, error)
	ReleaseReservation(ctx context.Context, in *ReleaseReservationRequest, opts ...grpc.CallOption) (*InventoryReply, error)
}

type inventoryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewInventoryServiceClient(cc grpc.ClientConnInterface) InventoryServiceClient {
	return &inventoryServiceClient{cc: cc}
}

func (c *inventoryServiceClient) CheckAvailability(ctx context.Context, in *InventoryRequest, opts ...grpc.CallOption) (*InventoryReply, error) {
	out := new(InventoryReply)
	if err := c.cc.Invoke(ctx, InventoryService_CheckAvailability_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inventoryServiceClient) ReserveItems(ctx context.Context, in *InventoryRequest, opts ...grpc.CallOption) (*InventoryReply, error) {
	out := new(InventoryReply)
	if err := c.cc.Invoke(ctx, InventoryService_ReserveItems_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inventoryServiceClient) ReleaseReservation(ctx context.Context, in *ReleaseReservationRequest, opts ...grpc.CallOption) (*InventoryReply, error) {
	out := new(InventoryReply)
	if err := c.cc.Invoke(ctx, InventoryService_ReleaseReservation_FullMethodName, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type InventoryServiceServer interface {
	CheckAvailability(context.Context, *InventoryRequest) (*InventoryReply, error)
	ReserveItems(context.Context, *InventoryRequest) (*InventoryReply, error)
	ReleaseReservation(context.Context, *ReleaseReservationRequest) (*InventoryReply, error)
}

type UnimplementedInventoryServiceServer struct{}

func (UnimplementedInventoryServiceServer) CheckAvailability(context.Context, *InventoryRequest) (*InventoryReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CheckAvailability not implemented")
}

func (UnimplementedInventoryServiceServer) ReserveItems(context.Context, *InventoryRequest) (*InventoryReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReserveItems not implemented")
}

func (UnimplementedInventoryServiceServer) ReleaseReservation(context.Context, *ReleaseReservationRequest) (*InventoryReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReleaseReservation not implemented")
}

func RegisterInventoryServiceServer(s grpc.ServiceRegistrar, srv InventoryServiceServer) {
	s.RegisterService(&InventoryService_ServiceDesc, srv)
}

func _InventoryService_CheckAvailability_Handler(
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	in := new(InventoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InventoryServiceServer).CheckAvailability(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InventoryService_CheckAvailability_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InventoryServiceServer).CheckAvailability(ctx, req.(*InventoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InventoryService_ReserveItems_Handler(
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	in := new(InventoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InventoryServiceServer).ReserveItems(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InventoryService_ReserveItems_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InventoryServiceServer).ReserveItems(ctx, req.(*InventoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InventoryService_ReleaseReservation_Handler(
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	in := new(ReleaseReservationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InventoryServiceServer).ReleaseReservation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: InventoryService_ReleaseReservation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InventoryServiceServer).ReleaseReservation(ctx, req.(*ReleaseReservationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var InventoryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "InventoryService",
	HandlerType: (*InventoryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CheckAvailability",
			Handler:    _InventoryService_CheckAvailability_Handler,
		},
		{
			MethodName: "ReserveItems",
			Handler:    _InventoryService_ReserveItems_Handler,
		},
		{
			MethodName: "ReleaseReservation",
			Handler:    _InventoryService_ReleaseReservation_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/inventory.proto",
}
