// Code generated for the demo gRPC contract. DO NOT EDIT.

package pb

type InventoryRequest struct {
	RequestId string `json:"request_id,omitempty"`
	Scenario  string `json:"scenario,omitempty"`
	SKU       string `json:"sku,omitempty"`
	Quantity  int32  `json:"quantity,omitempty"`
}

type ReleaseReservationRequest struct {
	RequestId     string `json:"request_id,omitempty"`
	Scenario      string `json:"scenario,omitempty"`
	ReservationId string `json:"reservation_id,omitempty"`
}

type InventoryReply struct {
	RequestId     string `json:"request_id,omitempty"`
	Scenario      string `json:"scenario,omitempty"`
	Success       bool   `json:"success,omitempty"`
	Status        string `json:"status,omitempty"`
	ReservationId string `json:"reservation_id,omitempty"`
	Warehouse     string `json:"warehouse,omitempty"`
	Available     int32  `json:"available,omitempty"`
}
