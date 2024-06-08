## 起動

* ホストが、全ステージを起動する
* 各ステージが、ホストに、`launched`イベントを発行する
* ホストが、全stageに、`begin_topic`イベントを発行する
    * 各ステージが、ホストに、`topic`イベントを通知する
        * topic
    * 各ステージが、ホストに、`end_topic`イベントを発行する
* ホストが、ExtractStageに、`begin_session`イベントを発行する
    * 実際受け取るのは、WatchStageのみ
* FileWatchStageが、ホストに、`source` イベントを発行する
    * パスとハッシュ
    * ホストが、パスとハッシュから同期情報を構成する
    * FileWatchStageが、全てのソースを送信したら、@finished`イベントを発行する
* ホストが、ExtractStageに`source`イベントを発行する
    * パス
* ExtractStageが、ホストに、`topic_payload`イベントを発行する
    * topicとpayload
    * ホストが、同期情報を更新する
        * 8種が変更されていれば破棄する
    * 全てのソースを通知し終えたら、`finished`イベントを発行する
* ホストが、全てのpayloadを受け取ったら、GenerateStageに、`topic_payload`イベントを発行する
    * topicとpayload
    * ホストが、GenerateStageに、ソースが変わるごとに`next_generate`イベントを発行する。
    * ホストが、GenerateStageに、全てのソースを通知し終えたら、最後に`end_generate`イベントを発行する
* GenerateStageが、コード生成する
    * 全てのコード生成を終えたら、ホストに`finished`イベントを発行する


## Stage

* Extract
    * PlaceholderExtractStage
        * SQL
        * パラメータと型
    * SelectListExtractStage
        * 列名と型
        * 起動時にschemaを受け取っておく必要がある
    * FileWatchStage
        * ファイルの発行と変更監視
* Generate
    * SqlGenerateStage
        * placeholderを置換したSQLを保存する
    * TypescriptTygeGenerateStage
        * typescriptのコードを吐く

## 通信チャネル

* ホスト -> ステージ (Pub/Sub)
* ステージ -> ホスト (Push/Pull)

## oneshot

`oneshot`で起動した場合。

* ホストが、ExtractStageから、`finished`イベントを受け取ったら、`quit`イベントを投げ返す
* ホストが、GenerateStageから、`finished`イベントを受け取ったら、`quit`イベントを投げ返す
* ホストが、全ステージから、`quit_accept`イベントを受け取ったら、井ペンとループを抜け出す

## 要検討

* ファイルの変更通知
    * WatchStageが、非同期で、ファイル変更の通知を受ける
        * WatchStageが、パスとハッシュを評価する
        * WatchStageが、ホストに、source イベントを発行する
* ホストで標準入力ハンドリング
    * `q`で終了
        * ホストが、全ステージに、`quit`イベントを発行する
            * ステージが、ホストに、`quit_accept`イベントを発行する
            * ステージを終了させる
* ログ
