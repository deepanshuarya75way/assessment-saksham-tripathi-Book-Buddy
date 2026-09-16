import os
import boto3
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from decimal import Decimal

# Load environment variables
load_dotenv()

app = Flask(__name__)
# Allow requests from the Next.js frontend
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Initialize AWS Services
# Boto3 will automatically use AWS credentials from environment variables:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
sns = boto3.client('sns', region_name=AWS_REGION)

# Table References
def get_books_table():
    return dynamodb.Table('Books')

def get_orders_table():
    return dynamodb.Table('Orders')

# Helper to convert float to Decimal for DynamoDB
def convert_floats_to_decimals(obj):
    if isinstance(obj, list):
        return [convert_floats_to_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    return obj

# Helper to convert Decimal to float for JSON response
def convert_decimals_to_floats(obj):
    if isinstance(obj, list):
        return [convert_decimals_to_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals_to_floats(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj

# --- API ROUTES ---

# Adding reviews to the existing project 
@app.route('/reviews', methods=['POST'])
def add_review():
    data=request.json

    book.id=data['bookId']
    user.id=data['userId']
    rating= int(data['rating'])
    review=data['review']

    if(rating <1 or rating >5):
        return jsonify({"error":"Rating must be 1-5"}),400
    # Check duplicate 
    existing=table.get_item(
        Key={
            "bookId":book_id,
            "reviewId":user_id
        }
    )
if("item" in existing):
    return jsonify({
        "error":"User already reviewed the book"
    }),409

# Blocking inappropriate_content
inappropriate_words=[
    "abuse",
    "spam",
    "scam",
    "hate"
]
is_inappropriate =any(
    word in review.lower()
    for word in inappropriate_words
)

status ="FLAGGED" if is_inappropriate else 
"PUBLISHED"

table.put_item(
    item={
        "bookId":book_id,
        "reviewId":review_id,
        "userId":user_id,
        "rating":rating,
        "review":review,
        "status":status,
        "createdAt":datetime.utcnow().isoformat()
    }
)

return jsonify({
    "message":(
        "Review message submitted for moderation"
        if is_inappropriate
        else "Review published"
    )
}), 201

# Not showing flags publically
reviews=[
    r for r in result['items']
    if r["status"=="PUBLISHED"
    ]
]

item={
    "bookId":book_id,
    "reviewId":user_id,
    "userId":user_id,
    "rating":rating,
    "review":review,
    "status":"PUBLISHED"
    "createdAt":datetime.utcnow().isoformat()
}

table.put_item(Item=item)
return jsonify({
    "message":"Review added",
    "review":item
}),201

# Get reviews 
@app.route('/reviews/<book_id>', methods=['GET'])
def get_reviews(book_id):
    result=table.query{
        KeyConditionExpress=boto3.dynamodb.conditions.Key("bookId").eq(book_id)
    }

    reviews=[
        r for r in result["items"]
        if r["status"]=="PUBLISHED"
    ]

    count= len(reviews)

    average=(
        sum(r["rating"] for r in reviews) /count
        if count else 0
    )

    return jsonify({
        "averageRating":round(average,1),
        "reviewCount":count,
        "reviews":reviews
    })

# Admin can hide or delete an review 
@app.route('/reviews/<book_id>/<review_id>/hide', methods=['PATCH'])
def hide_review(book_id,review_id):
    table.update-item{
        Key={
            "bookId":book_id,
            "reviewId":review_id
        },
        UpdateExpression="SET #s=:s",
        ExpressionAttributeValues={
            "#s":"status"
        },
        ExpressionAttributeValues={
            ":s":"HIDDEN"
        }
    }

    return jsonify({
        "message":"Review hidden"
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "message": "Flask backend connected to AWS!",
        "region": AWS_REGION
    })

@app.route('/api/books', methods=['GET'])
def get_books():
    """Fetch all books from DynamoDB"""
    try:
        table = get_books_table()
        response = table.scan()
        books = response.get('Items', [])
        return jsonify(convert_decimals_to_floats(books))
    except Exception as e:
        print(f"Error fetching books: {e}")
        # Fallback to a single mock item to prevent UI crash during setup
        return jsonify([{
            "id": "1", 
            "title": "Setup Required", 
            "author": "AWS DynamoDB", 
            "price": 0.00, 
            "image": "https://via.placeholder.com/400x600?text=Create+DynamoDB+Table",
            "previewUrl": "#
        }])

@app.route('/api/books', methods=['POST'])
def add_book():
    """Add a new book to DynamoDB"""
    data = request.json
    try:
        # Convert floats to Decimals for Boto3/DynamoDB
        data = convert_floats_to_decimals(data)
        
        table = get_books_table()
        table.put_item(Item=data)
        return jsonify({"message": "Book added to DynamoDB successfully", "book": data}), 201
    except Exception as e:
        print(f"Error adding book: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/orders/<user_id>', methods=['GET'])
def get_user_orders(user_id):
    """Fetch orders for a specific user from DynamoDB"""
    try:
        table = get_orders_table()
        # Querying with Partition Key 'orderId' is not efficient for user lookup 
        # unless we have a GSI. For now, we use a scan with FilterExpression.
        response = table.scan(
            FilterExpression="userId = :uid",
            ExpressionAttributeValues={":uid": user_id}
        )
        return jsonify(response.get('Items', []))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/orders', methods=['POST'])
def create_order():
    """Create a new order in DynamoDB and send SNS notification"""
    data = request.json
    # data should contain: orderId, userId, items, timestamp, status, deliveryDate, progress
    try:
        # Convert floats to Decimals for Boto3/DynamoDB
        data = convert_floats_to_decimals(data)
        
        # 1. Save to DynamoDB
        table = get_orders_table()
        table.put_item(Item=data)
        
        # 2. Send SNS Notification
        if SNS_TOPIC_ARN:
            try:
                message = f"New Order Placed!\nOrder ID: {data.get('orderId')}\nUser ID: {data.get('userId')}\nItems: {len(data.get('items', []))}\nTotal Items: {data.get('items')}"
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject="BookStore Order Notification"
                )
            except Exception as sns_err:
                print(f"SNS Failed (Check Topic ARN or Permissions): {sns_err}")
        
        return jsonify({"message": "Order created and notification sent", "order": data}), 201
    except Exception as e:
        print(f"Error creating order: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/books/<book_id>', methods=['DELETE'])
def delete_book(book_id):
    """Delete a book from DynamoDB"""
    try:
        table = get_books_table()
        table.delete_item(Key={'id': book_id})
        return jsonify({"message": "Book deleted successfully"}), 200
    except Exception as e:
        print(f"Error deleting book: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
