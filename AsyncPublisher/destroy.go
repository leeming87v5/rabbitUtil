package AsyncPublisher

import "ul_hashTaskEjector/destroyer"

// Destroy instance of Destroyer
// If GOGC=off you should call obj.Destroy() manually
func Destroy(d destroyer.Destroyer) {
	d.Destroy()
}
