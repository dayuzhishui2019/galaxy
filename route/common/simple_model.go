package common

type IdsParams struct {
	Ids string `json:"ids" binding:"required"`
}

type Page struct {
	PageNumber int `json:"pageNumber"`
	PageSize   int `json:"pageSize"`
}

func (p *Page) GetStart() int {
	return p.PageNumber * p.PageSize
}
