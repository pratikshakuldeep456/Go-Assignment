package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
)

type Object interface {
	GetKind() string
	GetID() string
	GetName() string
	SetID(string)
	SetName(string)
}

type Person struct {
	Name      string    `json:"name"`
	ID        string    `json:"id"`
	LastName  string    `json:"last_name"`
	Birthday  string    `json:"birthday"`
	BirthDate time.Time `json:"birth_date"`
}

func (p *Person) GetKind() string {
	return reflect.TypeOf(p).String()
}

func (p *Person) GetID() string {
	return p.ID
}

func (p *Person) GetName() string {
	return p.Name
}

func (p *Person) SetID(s string) {
	p.ID = s
}

func (p *Person) SetName(s string) {
	p.Name = s
}

type Animal struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Type    string `json:"type"`
	OwnerID string `json:"owner_id"`
}

func (a *Animal) GetKind() string {
	return reflect.TypeOf(a).String()
}

func (a *Animal) GetID() string {
	return a.ID
}

func (a *Animal) GetName() string {
	return a.Name
}

func (a *Animal) SetID(s string) {
	a.ID = s
}

func (a *Animal) SetName(s string) {
	a.Name = s
}

type ObjectDB interface {
	Store(ctx context.Context, object Object) error
	GetObjectByID(ctx context.Context, id string) (Object, error)
	GetObjectByName(ctx context.Context, name string) (Object, error)
	ListObjects(ctx context.Context, kind string) ([]Object, error)
	DeleteObject(ctx context.Context, id string) error
}

type RedisObjectDB struct {
	redisClient *redis.Client
}

func NewRedisObjectDB(client *redis.Client) *RedisObjectDB {
	return &RedisObjectDB{
		redisClient: client,
	}
}

func (db *RedisObjectDB) Store(ctx context.Context, object Object) error {
	objectBytes, err := json.Marshal(object)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", object.GetKind(), object.GetID())
	err = db.redisClient.Set(ctx, key, objectBytes, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (db *RedisObjectDB) GetObjectByID(ctx context.Context, id string) (Object, error) {
	objects, err := db.getObjectsByField(ctx, "ID", id)
	if err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, fmt.Errorf("object with ID '%s' not found", id)
	}

	return objects[0], nil
}

func (db *RedisObjectDB) GetObjectByName(ctx context.Context, name string) (Object, error) {
	objects, err := db.getObjectsByField(ctx, "Name", name)
	if err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, fmt.Errorf("object with name '%s' not found", name)
	}

	return objects[0], nil
}

func (db *RedisObjectDB) ListObjects(ctx context.Context, kind string) ([]Object, error) {
	iter := db.redisClient.Scan(ctx, 0, fmt.Sprintf("%s:*", kind), 0).Iterator()

	var objects []Object
	for iter.Next(ctx) {
		val, err := db.redisClient.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			return nil, err
		}

		var object Object
		err = json.Unmarshal(val, &object)
		if err != nil {
			return nil, err
		}

		objects = append(objects, object)
	}

	return objects, nil
}

func (db *RedisObjectDB) DeleteObject(ctx context.Context, id string) error {
	object, err := db.GetObjectByID(ctx, id)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", object.GetKind(), object.GetID())
	err = db.redisClient.Del(ctx, key).Err()
	if err != nil {
		return err
	}

	return nil
}

func (db *RedisObjectDB) getObjectsByField(ctx context.Context, field string, value string) ([]Object, error) {
	iter := db.redisClient.Scan(ctx, 0, "*", 0).Iterator()

	var objects []Object
	for iter.Next(ctx) {
		val, err := db.redisClient.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			return nil, err
		}

		var object Object
		err = json.Unmarshal(val, &object)
		if err != nil {
			return nil, err
		}

		objectValue := reflect.ValueOf(object).Elem().FieldByName(field).String()
		if objectValue == value {
			objects = append(objects, object)
		}
	}

	return objects, nil
}

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	objectDB := NewRedisObjectDB(redisClient)

	// Testing the implementation
	person := &Person{
		Name:     "John Doe",
		ID:       "123",
		LastName: "Doe",
		Birthday: "01-01-1990",
	}

	err := objectDB.Store(context.Background(), person)
	if err != nil {
		fmt.Println("Error storing person:", err)
		return
	}

	retrievedPerson, err := objectDB.GetObjectByID(context.Background(), "123")
	if err != nil {
		fmt.Println("Error retrieving person:", err)
		return
	}

	fmt.Println("Retrieved person:", retrievedPerson)

	animal := &Animal{
		Name:    "Rex",
		ID:      "456",
		Type:    "Dog",
		OwnerID: "123",
	}

	err = objectDB.Store(context.Background(), animal)
	if err != nil {
		fmt.Println("Error storing animal:", err)
		return
	}

	retrievedAnimal, err := objectDB.GetObjectByName(context.Background(), "Rex")
	if err != nil {
		fmt.Println("Error retrieving animal:", err)
		return
	}

	fmt.Println("Retrieved animal:", retrievedAnimal)

	objects, err := objectDB.ListObjects(context.Background(), reflect.TypeOf(person).String())
	if err != nil {
		fmt.Println("Error listing objects:", err)
		return
	}

	fmt.Println("List of persons:")
	for _, obj := range objects {
		fmt.Println(obj)
	}

	err = objectDB.DeleteObject(context.Background(), "123")
	if err != nil {
		fmt.Println("Error deleting object:", err)
		return
	}

	fmt.Println("Object deleted successfully")
}
