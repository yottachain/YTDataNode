package activeNodeList

import (
	"fmt"
	"testing"
)

func TestUpdate(t *testing.T) {
	Update()
	fmt.Println(nodeList)
	fmt.Println(HasNodeid("16Uiu2HAm9KXyHawLgpGDznx5JUk5RP516uuUiGumHtAjk11m9qSA"))
}
