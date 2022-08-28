package model

type User struct {
	Email string `dynamodbav:"email" json:"user_email"`
	Age   int    `dynamodbav:"age,omitempty" json:"age,omitempty"`
	City  string `dynamodbav:"city" json:"city"`
}
