package PrefetchConsumer

import "github.com/leeming87v5/rabbitUtil/destroyer"

// Destroy instance of Destroyer
// If GOGC=off you should call obj.Destroy() manually
func Destroy(d destroyer.Destroyer) {
	d.Destroy()
}
