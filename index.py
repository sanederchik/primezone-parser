from main import main
import json

def handler(e, ctx):
    try:
        main()
    except Exception as exc:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(exc)
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'success': True
        })
    }

if __name__ == '__main__':
    handler(1, 2)