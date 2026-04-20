package main

import (
	"context"
	"log"
	"net"

	"github.com/vika/global_ratio_mode/test-stand/demo/internal/common"
	"github.com/vika/global_ratio_mode/test-stand/demo/internal/pb"
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

	reply := &pb.InventoryReply{
		RequestId:     requestID,
		Scenario:      scenario,
		Success:       true,
		Status:        "reserved",
		ReservationId: "reservation-" + requestID[len(requestID)-6:],
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

func main() {
	service := common.Getenv("DD_SERVICE", "inventory-grpc-service")
	stopTracer := common.StartTracer(service)
	defer stopTracer()

	logger := common.NewLogger(service)
	server := common.NewGRPCServer(service)
	pb.RegisterInventoryServiceServer(server, &inventoryServer{logger: logger})

	listener, err := net.Listen("tcp", ":"+common.Getenv("GRPC_PORT", "50051"))
	if err != nil {
		log.Fatal(err)
	}

	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
