package main

import (
	"context"
	"net"

	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/common"
	"github.com/mydecisive/mdai-tracealyzer/test-stand/demo/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type inventoryServer struct {
	pb.UnimplementedInventoryServiceServer
	logger *common.Logger
}

func (s *inventoryServer) CheckAvailability(ctx context.Context, req *pb.InventoryRequest) (*pb.InventoryReply, error) {
	requestID, scenario := common.RequestMetadata(ctx, req.RequestId, req.Scenario)
	common.AnnotateGRPCSpan(ctx, pb.InventoryService_CheckAvailability_FullMethodName, requestID, scenario)
	s.logger.Info(ctx, "request received", map[string]any{
		"event":       "request_received",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/CheckAvailability",
	})

	reply := &pb.InventoryReply{
		RequestId: requestID,
		Scenario:  scenario,
		Success:   true,
		Status:    "available",
		Warehouse: "east",
		Available: 42,
	}

	s.logger.Info(ctx, "request completed", map[string]any{
		"event":       "request_completed",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/CheckAvailability",
	})
	return reply, nil
}

func (s *inventoryServer) ReserveItems(ctx context.Context, req *pb.InventoryRequest) (*pb.InventoryReply, error) {
	requestID, scenario := common.RequestMetadata(ctx, req.RequestId, req.Scenario)
	common.AnnotateGRPCSpan(ctx, pb.InventoryService_ReserveItems_FullMethodName, requestID, scenario)
	s.logger.Info(ctx, "request received", map[string]any{
		"event":       "request_received",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/ReserveItems",
	})

	if scenario == "checkout-grpc-error" {
		s.logger.Info(ctx, "request failed", map[string]any{
			"event":       "request_failed",
			"request_id":  requestID,
			"scenario":    scenario,
			"transport":   "grpc",
			"grpc.method": "InventoryService/ReserveItems",
			"grpc.code":   codes.Internal.String(),
		})
		return nil, status.Error(codes.Internal, "inventory reservation failed")
	}

	reply := &pb.InventoryReply{
		RequestId:     requestID,
		Scenario:      scenario,
		Success:       true,
		Status:        "reserved",
		ReservationId: common.ReservationID(requestID),
		Warehouse:     "east",
		Available:     41,
	}

	s.logger.Info(ctx, "request completed", map[string]any{
		"event":       "request_completed",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/ReserveItems",
	})
	return reply, nil
}

func (s *inventoryServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.InventoryReply, error) {
	requestID, scenario := common.RequestMetadata(ctx, req.RequestId, req.Scenario)
	common.AnnotateGRPCSpan(ctx, pb.InventoryService_ReleaseReservation_FullMethodName, requestID, scenario)
	s.logger.Info(ctx, "request received", map[string]any{
		"event":       "request_received",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/ReleaseReservation",
	})

	reply := &pb.InventoryReply{
		RequestId:     requestID,
		Scenario:      scenario,
		Success:       true,
		Status:        "released",
		ReservationId: req.ReservationId,
	}

	s.logger.Info(ctx, "request completed", map[string]any{
		"event":       "request_completed",
		"request_id":  requestID,
		"scenario":    scenario,
		"transport":   "grpc",
		"grpc.method": "InventoryService/ReleaseReservation",
	})
	return reply, nil
}

func runInventoryGRPC(service string, logger *common.Logger) error {
	server := common.NewGRPCServer(service)
	pb.RegisterInventoryServiceServer(server, &inventoryServer{logger: logger})

	listener, err := net.Listen("tcp", ":"+common.Getenv("GRPC_PORT", "50051"))
	if err != nil {
		return err
	}
	return server.Serve(listener)
}
