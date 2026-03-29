package service

import "strings"

const (
	DemoUsername = "student"
	DemoPassword = "student"
	DemoToken    = "demo-token"
	DemoSubject  = "student"
)

type AuthService struct{}

func NewAuthService() *AuthService {
	return &AuthService{}
}

func (s *AuthService) Login(req LoginRequest) (LoginResponse, bool) {
	if req.Username == DemoUsername && req.Password == DemoPassword {
		return LoginResponse{
			AccessToken: DemoToken,
			TokenType:   "Bearer",
		}, true
	}

	return LoginResponse{}, false
}

func (s *AuthService) VerifyAuthorizationHeader(authHeader string) VerifyResponse {
	const prefix = "Bearer "

	if !strings.HasPrefix(authHeader, prefix) {
		return VerifyResponse{
			Valid: false,
			Error: "unauthorized",
		}
	}

	token := strings.TrimPrefix(authHeader, prefix)
	if token != DemoToken {
		return VerifyResponse{
			Valid: false,
			Error: "unauthorized",
		}
	}

	return VerifyResponse{
		Valid:   true,
		Subject: DemoSubject,
	}
}
